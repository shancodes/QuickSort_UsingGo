package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

const N = 1 << 22   // Size of Input Array - to be sorted using QuickSort
var MAX_THREADS int // Max Threads to be used for Parallel Processing

var threadCount int // Current Thread Count
var finalCount int
var finalCountSync int

/*
* Triggers Sequential QuickSort
* Passes a Single worker channel that is never read
* When sort is done, isSorted is called and result is written to finalCount
 */
func QuickSortSequential(s []float64, doneChan chan bool) {
	worker := make(chan int)
	// Pass in a single worker channel and nil for
	QuickSort(s, 0, N-1, nil, worker)
	doneChan <- isSorted(s)
}

/*
* Triggers Parallel QuickSort
* Workers are represented with int channels
* MAX_Threads number of channels are created
* When all threads are done, isSorted is called and result is written to
 */
func QuickSortParallel(s []float64, donechan chan bool) {
	workers := make(chan int, MAX_THREADS-1)
	for i := 0; i < (MAX_THREADS - 1); i++ {
		workers <- 1
	}
	QuickSort(s, 0, N-1, nil, workers)
	donechan <- isSorted(s)
}

/*
* Runs Quick Sort Parallely
* Takes in a float64 array, low and high for range
* done channel to signal done
* worker channel for parallelism 
*/
func QuickSort(s []float64, low, high int, done chan int, workers chan int) {
	// Used to Signal done status of execution
	if done != nil {
		defer func() { done <- 1 }()
	}

	if low >= high {
		return
	}
	// Using Buffered channel to not wait on read
	doneChannel := make(chan int, 1)

	pivot := partition(s, low, high)

	select {
	case <-workers:
		// Spawn new Go Routine if worker available
		go QuickSort(s, pivot+1, high, doneChannel, workers)
	default:
		// Run Sequentially - no worker available
		QuickSort(s, pivot+1, high, nil, workers)
		// calling this here as opposed to using the defer
		doneChannel <- 1
	}
	// Recursively call right half of the array, next to pivot
	QuickSort(s, low, pivot-1, nil, workers)
	// Wait for the GoRoutine at this level - Blocks if we spawned a new go routine
	<-doneChannel
	return
}

/*
* Returns Pivot based on which array is split
* All elements less than pivot are put to the left and all greater than pivot are to the right
*/
func partition(A []float64, low, high int) int {

	// Pick a low as pivot
	var pivot = A[low]
	var i = low
	var j = high

	for i < j {

		// Increment the index value of "i" till the left most <= pivot value
		for A[i] <= pivot && i < high {
			i++
		}

		// Decrement the index value of "j" till the right most values >= pivot value
		for A[j] > pivot && j > low {
			j--
		}

		// swap values at i and j 
		if i < j {
			var temp = A[i]
			A[i] = A[j]
			A[j] = temp
		}
	}

	// Value at j becomes value at low
	A[low] = A[j]
	// Value at pivot becomes value at j
	A[j] = pivot

	// Return index of pivot ie: j 
	return j
}

/*
* Random Array Generator - Runs in a separate GoRoutine
* Takes a channel of type []float64 as input
* Generates random array of size N and adds it to the channel
 */
func generateRandomArray(inputChan chan []float64) {
	rand.Seed(time.Now().Unix())
	for {
		randomArray := make([]float64, N)
		for i := 0; i < N; i++ {
			randomArray[i] = rand.Float64()
		}
		inputChan <- randomArray
	}
}

/*
* Function to test if the Input has been sorted
 */
func isSorted(inputArray []float64) bool {
	if inputArray == nil {
		return false
	}

	last := inputArray[0]
	for i := 1; i < N; i++ {
		if inputArray[i] < last {
			return false
		}
	}

	return true
}

/*
* Increments FinalCount - Variable that tracks the number of sorted Arrays
* Runs in a Separate GoRoutine
 */
func countCompletedSorts(done, exit chan bool) {
loop:
	for {
		select {
		case val := <-done:
			if val {
				finalCount += 1
			}
		case <-exit:
			break loop
		default:
		}
	}
}

func main() {
	fmt.Println("CPSC 5600 - Final Project :: Parallel QuickSort")

	// Start Random Array generator GoRoutine
	sortInputChannel := make(chan []float64)
	go generateRandomArray(sortInputChannel)

	// Start Counter GoRoutine
	doneChan := make(chan bool)
	exitChan := make(chan bool)
	go countCompletedSorts(doneChan, exitChan)

	// Get User Options
	option, err := fetchOptions()

	if err != nil {
		fmt.Println(err)
		fmt.Println("Please Select from Available Options")
		return
	}

	// Process Based on User Options
	switch option {
	case 1:
		err := setInitialVariables()

		if err != nil {
			fmt.Println(err)
			return
		}
		resetCounter()
		runAsync(sortInputChannel, exitChan, doneChan)
	case 2:
		resetCounter()
		runSync(sortInputChannel, exitChan, doneChan)
	default:
		fmt.Println("Option Not listed")
	}
}

/*
* Timed Loop to run QuickSortSequential
 */
func runSync(sortInputChannel chan []float64, exitChan, doneChan chan bool) {
	runtime.GOMAXPROCS(1)
loop:
	for timeout1 := time.After(10 * time.Second); ; {
		select {
		case <-timeout1:
			exitChan <- true
			break loop
		case arr := <-sortInputChannel:
			QuickSortSequential(arr, doneChan)
		default:
		}
	}

	fmt.Println(fmt.Sprintf("%d arrays of size %d were sorted in 10 seconds", finalCount, N))
}

/*
* Timed Loop to run QuickSortParallel
 */
func runAsync(sortInputChannel chan []float64, exitchan, donechan chan bool) {
loop:
	for timeout := time.After(10 * time.Second); ; {
		select {
		case <-timeout:
			exitchan <- true
			break loop
		case arr := <-sortInputChannel:
			QuickSortParallel(arr, donechan)
		default:
		}
	}

	fmt.Println(fmt.Sprintf("%d arrays of size %d were sorted in 10 seconds", finalCount, N))
}

/*
* Sets MaxGoProcs for CPU and MaxThreads for Maximum Parallelism - from User
* Only only when choosing to run Parallel version of QuickSort
 */
func setInitialVariables() error {
	cores := runtime.NumCPU()
	fmt.Printf("This Machine has %d CPUs available\nEnter number of CPUs to use (1 - %d): ", cores, cores)
	var procs int
	_, err := fmt.Scanf("%d", &procs)
	if err != nil {
		return err
	}

	runtime.GOMAXPROCS(procs)
	fmt.Println(fmt.Sprintf("%d cores - %d MaxProcs", cores, runtime.GOMAXPROCS(procs)))

	fmt.Printf("Enter the MaxThreads for running Quick Sort : ")
	_, err = fmt.Scanf("%d", &MAX_THREADS)
	if err != nil {
		return err
	}

	return nil
}

/*
* Displays Options Message to User
 */
func fetchOptions() (int, error) {
	var userOption int
	fmt.Println("1. Run Parallel QuickSort for 10 Seconds")
	fmt.Println("2. Run Synchronous QuickSort for 10 Seconds")

	_, err := fmt.Scanf("%d", &userOption)

	if err != nil {
		return -1, err
	}

	return userOption, nil
}

/*
* Resets finalCount
 */
func resetCounter() {
	finalCount = 0
}
