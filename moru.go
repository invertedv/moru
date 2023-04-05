// Package moru provides functions to run the models created by [goMortgage].
//
// Using moru is quite straightforward.
//
// There are two options: ScoreToTable and ScoreToPipe.
//
// With ScoreToTable, the user provides
//
//   - A ClickHouse table that has the features required by the model(s)
//   - Pointers to the directories of the models created by goMortgage
//
// The input table, augmented by the model outputs, is saved back to a new ClickHouse table.
//
// With ScoreToPipe, the user provides
//
//   - A seafan.Pipeline with model features
//   - Pointers to the directories of the models created by goMortgage
//
// The model outputs are added to the pipeline.
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

// ModelDef defines a model and the fields to calculate from it.  The models are run in index order.
type ModelDef struct {
	Location     string   // directory with the procyonb model
	FieldNames   []string // slice of field names we're calculating
	FieldColumns [][]int  // columns to sum corresponding to the field names
}

func (mdef *ModelDef) Error() error {
	if mdef.FieldNames == nil || mdef.FieldColumns == nil {
		return fmt.Errorf("one of ModelDef FieldNames or FieldColumns is nil")
	}

	if len(mdef.FieldColumns) != len(mdef.FieldNames) {
		return fmt.Errorf("ModelDef FieldColumns and FieldDefs aren't the same length %v", mdef.FieldNames)
	}

	return nil
}

// ScoreToTable creates destTable from sourceTable adding fitted values from one or more models.
//   - sourceTable: source ClickHouse table with features required by the model
//   - destTable: created ClickHouse table with sourceTable fields plus model outputs
//   - orderBy: comma-separated values of sourceTable that create a unique key
//   - models: model specifications (location, field names and columns)
//   - batchsize: number of rows to process as a group
//   - nWorker: number of concurrent processes
//   - conn: ClickHouse connector
//
// Set 	sea.Verbose = false to suppress messages during run.
func ScoreToTable(sourceTable, destTable, orderBy string, models []ModelDef, batchSize, nWorker int, conn *chutils.Connect) error {
	var (
		pipe sea.Pipeline
		err  error
	)

	for _, mdef := range models {
		if e := mdef.Error(); e != nil {
			return e
		}
	}

	if pipe, err = NewPipe(sourceTable, orderBy, models, 1, batchSize, conn); err != nil {
		return err
	}

	if ex := MakeTable(destTable, orderBy, pipe, conn); ex != nil {
		return ex
	}

	// # of rows in the table
	rows := Rows(sourceTable, conn)
	if batchSize == 0 {
		batchSize = rows
	}

	if nWorker == 0 {
		nWorker = runtime.NumCPU()
	}

	// # of times we have to query the table
	queueLen := rows / batchSize
	if rows%batchSize > 0 {
		queueLen++
	}

	// check if have more workers than needed
	if queueLen < nWorker {
		nWorker = queueLen
	}

	c := make(chan error)

	running := 0
	for ind := 0; ind < queueLen; ind++ {
		startRow := ind * batchSize
		go func() {
			var (
				pipeX sea.Pipeline
				err   error
			)

			if pipeX, err = NewPipe(sourceTable, orderBy, models, startRow, batchSize, conn); err != nil {
				c <- err
				return
			}
			c <- InsertTable(destTable, pipeX, conn)
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

// ScoreToPipe adds the model outputs to pipe.  Note: the maps for categorical features should not ever use
// a default value in its sea.FType as this compression can cause the model output to be wrong.  The default values
// will be applied in ScoreToPipe.
//   - pipe: pipeline to add model outputs to.
//   - models: definitions of models to add
//   - ftMods: slice of features for each model.  if nil, ScoreToPipe will build this & return it.
//   - obsFts: FType of the targets of models.
func ScoreToPipe(pipe sea.Pipeline, models []ModelDef, ftMods []sea.FTypes, obsFts sea.FTypes) error {
	// if ftMods is nil, build the slice up from the files
	if ftMods == nil {
		var err error
		ftMods, obsFts, _, err = GatherFts(models)
		if err != nil {
			return nil
		}
	}

	for ind, modl := range models {
		if e := addAllModels(modl.Location, pipe, ftMods[ind], obsFts[ind], modl); e != nil {
			return e
		}
	}

	return nil
}

// NewPipe creates a new data pipeline from "table" and appends the model outputs specified by "models".
// The pipeline consists of rows startRow to startRow+batchSize-1 of table. The unique key orderBy is needed so
// that ClickHouse will correctly run through the table over multiple calls to NewPipe.
//
// Arguments:
//   - table: name of the ClickHouse table with data to calculate model
//   - orderBy: comma-separated field list that produces a unique key
//   - models: model location and fields to create
//   - startRow: first row of table to pull for the pipeline
//   - batchSize: number of rows of table to pull
//   - conn: connector to ClickHouse
func NewPipe(table, orderBy string, models []ModelDef, startRow, batchSize int, conn *chutils.Connect) (sea.Pipeline, error) {
	with := fmt.Sprintf("WITH d AS (SELECT * FROM %s ORDER BY %s)", table, orderBy)
	qry := fmt.Sprintf("%s SELECT *, toInt32(rowNumberInAllBlocks()) AS rn FROM d WHERE rn >= %d AND rn < %d ",
		with, startRow, startRow+batchSize)

	rdr := s.NewReader(qry, conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return nil, e
	}
	ftMods, obsFts, fts, err := GatherFts(models)
	if err != nil {
		return nil, err
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
		if e := addAllModels(modl.Location, pipe, ftMods[ind], obsFts[ind], modl); e != nil {
			return nil, e
		}
	}

	return pipe, nil
}

// GatherFts collects the FTypes of the fields in models
//   - models: models to collect FTypes for.
//
// Returns:
//   - ftMods: the features (as sea.FTypes) in each model, same length as models
//   - obsFts: the sea.FTypes of the target for each model.  If the target is normalized, then this is needed to
//     unnormalize the model output.
//   - cats: list of the cats in all the models.  This is needed to make sure all cat features are treated as such.
//     This is not a problem for strings, but ins default to FRCts not FRCat.
func GatherFts(models []ModelDef) (ftMods []sea.FTypes, obsFts, cats sea.FTypes, err error) {
	// values by model: features and targets
	ftMods = make([]sea.FTypes, len(models))
	obsFts = make([]*sea.FType, len(models))

	for ind, modl := range models {
		loc := slash(modl.Location)
		modSpec, e := sea.LoadModSpec(loc + "modelS.nn")
		if e != nil {
			return nil, nil, nil, e
		}

		ftModl, e := sea.LoadFTypes(loc + "fieldDefs.jsn")
		if e != nil {
			return nil, nil, nil, e
		}

		for _, ft := range ftModl {
			if ft.Name == modSpec.TargetName() || ft.Name+"Oh" == modSpec.TargetName() {
				// target
				obsFts[ind] = ft
			} else {
				// features
				ftMods[ind] = append(ftMods[ind], ft)

				// add to cats if not FRCts
				if ft.Role != sea.FRCts && cats.Get(ft.Name) == nil {
					ftNew := &sea.FType{
						Name: ft.Name,
						Role: ft.Role,
					}

					cats = append(cats, ftNew)
				}
			}
		}
	}

	return ftMods, obsFts, cats, nil
}

// addAllModels runs through all the models in the inputModels directory.
func addAllModels(rootDir string, basePipe sea.Pipeline, fts sea.FTypes, obsFt *sea.FType, modl ModelDef) error {
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

	for ind := 0; ind < len(modl.FieldNames); ind++ {
		// AddFitted will use fts in place of whatever we have in basePipe fts
		if e := sea.AddFitted(basePipe, rootDir+"model", modl.FieldColumns[ind], modl.FieldNames[ind], fts, false, obsFt); e != nil {
			return e
		}
	}

	return nil
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
