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

// Unfortunately, this example is not made testable because it inherently
// operates as a race condition between two independent things. To get a
// deterministic test, the `resolve` would need to be triggered by the periodic
// task, making for an extremely contrived example. This example errs on the
// side of providing a more standard use case.
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
}
