## package moru
[![Go Report Card](https://goreportcard.com/badge/github.com/invertedv/moru)](https://goreportcard.com/report/github.com/invertedv/moru)
[![godoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/mod/github.com/invertedv/moru?tab=overview)

Package moru provides functions to run the models created by [goMortgage](https://pkg.go.dev/github.com/invertedv/goMortgage).
Using moru is quite straightforward. The user provides 

- A ClickHouse table that has the features required by the model.
- A pointer to the directory of the model created by goMortgage.

The input table, augmented by the model outputs, is saved back to ClickHouse.



