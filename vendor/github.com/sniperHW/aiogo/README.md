# aiogo

High performance async-io networking for Golang in proactor mode

	go test -v -run=^$ -bench BenchmarkEcho128KParallel -count 10
	go test -v -run=^Test8K$ -count 10 -cpuprofile=cpu.out	