// Package moru provides functions to run the models created by [goMortgage].
//
// Using moru is quite straightforward. The user provides
//
//   - A ClickHouse table that has the features required by the model.
//
//   - A pointer to the directory of the model created by goMortgage.
//
// The input table, augmented by the model outputs, is saved back to ClickHouse.
//
// [goMortgage]: https://pkg.go.dev/github.com/invertedv/goMortgage
package moru

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"

	sea "github.com/invertedv/seafan"
)

// for strconv.ParseInt
const (
	base10 = 10
	bits32 = 32
)

// ModelMap maps the outputs of the model to fields in the output table.
// Examples:
//
//   - map["yhat"] = []int{0}
//
//     creates a new field "yhat" that is the first column of the model output.
//
//   - map["yhat01"] = []int{0,1}
//
//     creates a new field "yhat01" that is the sum of the first two columns of the model output.
//
// If the model is a
// regression, use the first example as a template (i.e. column 0 is the output).
type ModelMap map[string][]int

// ModelDef defines a model and the fields to calculate from it.
type ModelDef struct {
	Location string   // directory with the goMortgage model
	Fields   ModelMap // map of fields names to columns of model output
}

// addModel adds the output of an addModel model to basePipe. This expects 4 files in modelRoot:
//   - The NNModel files modelP.nn and modelS.nn
//   - FTypes file that defines the features in the model.  The data in basePipe is re-normalized and re-mapped using
//     these values.
//   - target.specs.  This file specifies the name(s) of the fields to create in basePipe. It has the format:
//     <field name>:<target columns to coalesce separated by commas>.
func addModel(modelRoot string, basePipe sea.Pipeline) error {
	modelRoot = slash(modelRoot)

	// see if there are any directories in here -- these would be input models to this model
	dirList, e := os.ReadDir(modelRoot)
	if e != nil {
		return e
	}

	hasFiles := false // this directory may be a directory of directories (submodels)
	for _, entry := range dirList {
		// load up the submodel
		if entry.IsDir() {
			if er := addModel(modelRoot+entry.Name(), basePipe); er != nil {
				return er
			}
		} else {
			hasFiles = true
		}
	}

	if !hasFiles {
		return nil
	}

	fts, e := sea.LoadFTypes(modelRoot + "fieldDefs.jsn")
	if e != nil {
		return e
	}

	handle, e := os.Open(modelRoot + "targets.spec")
	if e != nil {
		return e
	}

	rdr := bufio.NewReader(handle)

	for line, err := rdr.ReadString('\n'); err == nil; line, err = rdr.ReadString('\n') {
		spl := toSlice(line, "{")
		if len(spl) != 2 {
			return fmt.Errorf("existing model %s error in target %s", modelRoot, line)
		}

		lvls := strings.Split(strings.ReplaceAll(spl[1], "}", ""), ",")
		fieldName := spl[0]
		targets := make([]int, 0)

		for _, lvl := range lvls {
			ilvl, e1 := strconv.ParseInt(lvl, base10, bits32)
			if e1 != nil {
				return fmt.Errorf("existing error parsing targets %s for model %s", line, modelRoot)
			}
			targets = append(targets, int(ilvl))
		}

		modSpec, e := sea.LoadModSpec(modelRoot + "modelS.nn")
		if e != nil {
			return e
		}
		var obsFt *sea.FType = nil

		if trg := modSpec.TargetName(); trg != "" {
			obsFt = fts.Get(trg)
		}

		// AddFitted will use fts in place of whatever we have in basePipe fts
		if e := sea.AddFitted(basePipe, modelRoot+"model", targets, fieldName, fts, true, obsFt); e != nil {
			return e
		}
	}

	return nil
}

// addAllModels runs through all the models in the inputModels directory.
func addAllModels(rootDir string, basePipe sea.Pipeline, fts sea.FTypes, obsFt *sea.FType, modl ModelMap) error {
	rootDir = slash(rootDir)
	entries, e := os.ReadDir(rootDir)
	if e != nil {
		return e
	}

	// cycle through input models
	for _, dir := range entries {
		if !dir.IsDir() {
			continue
		}

		if e1 := addModel(rootDir+dir.Name(), basePipe); e1 != nil {
			return e1
		}
	}

	for fieldName, targets := range modl {
		// AddFitted will use fts in place of whatever we have in basePipe fts
		if e := sea.AddFitted(basePipe, rootDir+"model", targets, fieldName, fts, false, obsFt); e != nil {
			return e
		}
	}

	return nil
}

// NewPipe creates a new data pipeline.
//   - table: name of the ClickHouse table with data to calculate model
//   - orderBy: comma-separated field list that produces a unique key
//   - models: model location and fields to create
//   - startRow: first row of table to pull for the pipeline
//   - batchSize: number of rows of table to pull
//   - conn: connector to ClickHouse
func NewPipe(table, orderBy string, models []ModelDef, startRow, batchSize int, conn *chutils.Connect) (sea.Pipeline, error) {
	with := fmt.Sprintf("WITH d AS (SELECT * FROM %s ORDER BY %s)", table, orderBy)
	qry := fmt.Sprintf("%s SELECT *, 'N' AS aoBap, toInt32(rowNumberInAllBlocks()) AS rn FROM d WHERE rn >= %d AND rn < %d ",
		with, startRow, startRow+batchSize)

	rdr := s.NewReader(qry, conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return nil, e
	}

	// fts is all the fields in either model that are categorical.  This is to force pipe.Init to treat them as such.
	// We don't specify the levels, this way we'll get a granular map of all the values in the data so that the
	// application of the maps of each model will be correct.
	var fts sea.FTypes

	// values by model: features and targets
	ftMods := make([]sea.FTypes, len(models))
	obsFts := make([]*sea.FType, len(models))

	for ind, modl := range models {
		loc := slash(modl.Location)
		modSpec, e := sea.LoadModSpec(loc + "modelS.nn")
		if e != nil {
			return nil, e
		}

		ftModl, e := sea.LoadFTypes(loc + "fieldDefs.jsn")
		if e != nil {
			return nil, e
		}
		for _, ft := range ftModl {
			if ft.Name == modSpec.TargetName() || ft.Name+"Oh" == modSpec.TargetName() {
				// target
				obsFts[ind] = ft
			} else {
				// features
				ftMods[ind] = append(ftMods[ind], ft)
				if ft.Role != sea.FRCts && fts.Get(ft.Name) == nil {
					ftNew := &sea.FType{
						Name: ft.Name,
						Role: ft.Role,
					}
					fts = append(fts, ftNew)
				}
			}
		}
	}

	pipe := sea.NewChData("model run")
	sea.WithReader(rdr)(pipe)
	sea.WithBatchSize(0)(pipe)
	sea.WithFtypes(fts)(pipe)
	if e := pipe.Init(); e != nil {
		return nil, e
	}

	// add the models.
	for ind, modl := range models {
		if e := addAllModels(modl.Location, pipe, ftMods[ind], obsFts[ind], modl.Fields); e != nil {
			return nil, e
		}
	}

	return pipe, nil
}

// MakeTable makes ClickHouse table tableName based on the fields in pipe. MakeTable overwrites the table if it
// exists.
//   - tableName: name of ClickHouse table to create
//   - orderBy: comma-separated values of sourceTable that create a unique key
//   - pipe: Pipeline containing fields to create for the table
//   - conn: connector to ClickHouse
func MakeTable(tableName, orderBy string, pipe sea.Pipeline, conn *chutils.Connect) error {
	gd := pipe.GData()
	tb := gd.TableSpec()
	tb.Key = orderBy

	if e := tb.Create(conn, tableName); e != nil {
		return e
	}

	return nil
}

// InsertTable inserts the data in pipe into the ClickHouse table tableName. The table must already exist.
//   - tableName: name of table to insert into (table must exist)
//   - pipe: pipeline with data to insert
//   - conn: ClickHouse connector
func InsertTable(tableName string, pipe sea.Pipeline, conn *chutils.Connect) error {
	wtr := s.NewWriter(tableName, conn)
	defer func() { _ = wtr.Close() }()

	if e := chutils.Export(pipe.GData(), wtr, 0, false); e != nil {
		return e
	}

	return nil
}

// Rows returns the number of rows in a ClickHouse table.
//   - tableName: name of table to for row count
//   - conn: ClickHouse connector
func Rows(tableName string, conn *chutils.Connect) int {
	qry := fmt.Sprintf("SELECT count(*) FROM %s", tableName)

	res, e := conn.Query(qry)
	if e != nil {
		return 0
	}
	defer func() { _ = res.Close() }()

	var rows int
	res.Next()
	if e := res.Scan(&rows); e != nil {
		return 0
	}

	return rows
}

// Score creates destTable from sourceTable adding fitted values from one or more models.
//   - sourceTable: source ClickHouse table with features required by the model
//   - destTable: created ClickHouse table with sourceTable fields plus model outputs
//   - orderBy: comma-separated values of sourceTable that create a unique key
//   - models: model specifications (location, field names and columns)
//   - batchsize: number of rows to process as a group
//   - nWorker: number of concurrent processes
//   - conn: ClickHouse connector
//
// Set 	sea.Verbose = false to suppress messages during run.
func Score(sourceTable, destTable, orderBy string, models []ModelDef, batchSize, nWorker int, conn *chutils.Connect) error {
	var pipe sea.Pipeline
	var e error

	if pipe, e = NewPipe(sourceTable, orderBy, models, 1, batchSize, conn); e != nil {
		return e
	}

	if ex := MakeTable(destTable, orderBy, pipe, conn); ex != nil {
		return ex
	}

	rows := Rows(sourceTable, conn)
	if batchSize == 0 {
		batchSize = rows
	}

	if nWorker == 0 {
		nWorker = runtime.NumCPU()
	}

	queueLen := rows / batchSize
	if rows%batchSize > 0 {
		queueLen++
	}

	if queueLen < nWorker {
		nWorker = queueLen
	}

	c := make(chan error)

	running := 0
	for ind := 0; ind < queueLen; ind++ {
		startRow := ind * batchSize
		if pipe, e = NewPipe(sourceTable, orderBy, models, startRow, batchSize, conn); e != nil {
			return e
		}
		go func() {
			c <- InsertTable(destTable, pipe, conn)
		}()
		running++
		if running == nWorker {
			e := <-c
			if e != nil {
				return e
			}
			running--
		}
	}
	// Wait for queue to empty
	for running > 0 {
		e := <-c
		if e != nil {
			return e
		}
		running--
	}

	return nil
}

// slash appends a trailing backslash if there is not one
func slash(path string) string {
	if path[len(path)-1:] == "/" {
		return path
	}

	return path + "/"
}

// toSlice returns a slice by splitting str on sep
func toSlice(str, sep string) []string {
	str = strings.ReplaceAll(str, " ", "")
	str = strings.ReplaceAll(str, "\n", "")

	// check for no entries
	if str == "" {
		return nil
	}
	return strings.Split(str, sep)
}
