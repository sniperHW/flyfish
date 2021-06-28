package gopool

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go test -v -run=^$ -bench Benchmark -count 10
//go tool cover -html=coverage.out
import (
	"fmt"
	//"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestGo(t *testing.T) {
	{
		pool := New(Option{
			MaxRoutineCount: 10,
			Mode:            QueueMode,
		})

		var wait sync.WaitGroup

		fmt.Println("1")

		wait.Add(20)

		for i := 0; i < 20; i++ {
			pool.Go(func() {
				time.Sleep(time.Millisecond * 5)
				wait.Done()
			})
		}

		wait.Wait()

		fmt.Println("3")

		wait.Add(20)

		for i := 0; i < 20; i++ {
			pool.Go(func() {
				time.Sleep(time.Millisecond * 5)
				wait.Done()
			})
		}

		wait.Wait()

		wait.Add(120)

		for i := 0; i < 120; i++ {
			pool.Go(func() {
				time.Sleep(time.Millisecond * 5)
				wait.Done()
			})
		}

		wait.Wait()

		fmt.Println(pool.routineCount)

		pool.Close()

	}

	{
		pool := New(Option{
			MaxRoutineCount: 10,
			Mode:            GoMode,
		})

		var wait sync.WaitGroup

		wait.Add(20)

		for i := 0; i < 20; i++ {
			pool.Go(func() {
				time.Sleep(time.Millisecond * 5)
				wait.Done()
			})
		}

		wait.Wait()
	}
}

func BenchmarkGoroutine(b *testing.B) {
	pool := New(Option{
		MaxRoutineCount: 1024,
		Mode:            QueueMode,
	})

	var wait sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wait.Add(1)
		pool.Go(func() {
			wait.Done()
		})
	}
	wait.Wait()

	pool.Close()
}

func BenchmarkRoutine(b *testing.B) {
	var wait sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wait.Add(1)
		go func() {
			wait.Done()
		}()
	}
	wait.Wait()
}
