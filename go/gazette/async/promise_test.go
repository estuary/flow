package async

import (
	"fmt"
	"time"
)

func ExamplePromise_Wait() {
	var p = make(Promise)

	go func() {
		// Do async work.
		time.Sleep(10 * time.Millisecond)
		fmt.Println("Async routine completes.")
		p.Resolve()
	}()

	fmt.Println("Pre-wait logic runs.")
	p.Wait()
	fmt.Println("Post-wait logic runs.")

	// Output:
	// Pre-wait logic runs.
	// Async routine completes.
	// Post-wait logic runs.
}

// Unfortunately this example uses a race condition to keep the example simple
// and straightforward. Although unlikely in practice, it is possible for this
// test to fail.
func ExamplePromise_WaitWithPeriodicTask() {
	var p = make(Promise)

	go func() {
		// Do async work.
		time.Sleep(50 * time.Millisecond)
		p.Resolve()
	}()

	var i int
	p.WaitWithPeriodicTask(11*time.Millisecond, func() {
		i += 1
		fmt.Printf("%d\n", i)
	})

	// Output:
	// 1
	// 2
	// 3
	// 4
}
