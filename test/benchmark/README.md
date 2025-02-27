Run benchmarks individually with:

```
 go test -bench=BenchmarkRandomReadBson -benchtime=1x  ./scale_test.go -v
```

Where -bench is the name of the benchmark function

Write tests must be done before read tests so that data is available for read tests to read

The below command should run all tests

```
go test -bench=. -benchtime=1x . -v
```
