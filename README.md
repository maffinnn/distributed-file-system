# distributed-file-system


1. To expose the directories to clients at the server:
```
export EXPORT_ROOT_PATHS="your/path/to/distributed-file-system/etc/exports"
```

2. To run the test services, different senarios are included in the `test.go` file. Manually changing the test cases is required.
```
go run test/test.go
```
