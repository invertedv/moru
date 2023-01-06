## package moru
[![Go Report Card](https://goreportcard.com/badge/github.com/invertedv/moru)](https://goreportcard.com/report/github.com/invertedv/moru)
[![godoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/mod/github.com/invertedv/moru?tab=overview)

Package moru provides functions to run the models created by [goMortgage](https://pkg.go.dev/github.com/invertedv/goMortgage).
Using moru is quite straightforward. 

There are two options: ScoreToTable and ScoreToPipe.

With ScoreToTable, the user provides 

- A ClickHouse table that has the features required by the model(s)
- Pointers to the directories of the models created by goMortgage

The input table, augmented by the model outputs, is saved back to a new ClickHouse table.

With ScoreToPipe, the user provides

- A seafan.Pipeline with model features
- Pointers to the directories of the models created by goMortgage.

The model outputs are added to the pipeline.
