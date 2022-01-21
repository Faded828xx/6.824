# lab1 Map Reduce test shell script
# written by Faded828x

cd src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so