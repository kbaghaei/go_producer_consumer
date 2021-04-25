/*
Author: Kourosh T. Baghaei
April 2021

This Go program calls a python script across different processes. 
And assigns tasks to each instance.

(Refer to the licence attached to this project)
*/

package main

import(
	"fmt"
	"os"
	"os/exec"
	"log"
	"io/ioutil"
	"bufio"
	"strconv"
	"bytes"
	"time"
	_"math/rand"
	"sync"
)

type MLTask struct {
	startIndex int
	batchesCount int
}

const (
	main_menu = iota
	settings
)

func spawn_pytorch(worker_index int, task MLTask) bool {
	task_index, batches_count := task.startIndex, task.batchesCount
	// Create external command obj:
	c := exec.Command("cmd", "/C", "python_job.bat")
	c.Env = os.Environ()
	c.Env = append(c.Env, fmt.Sprintf("CONSUMER_WORKER=%d",worker_index))
	stdout, err0 := c.StdoutPipe()		
	if err0 != nil {
		log.Fatal(err0)
		fmt.Println("(x) MPOL ERROR: StdoutPipe to external command failed! worker_index: ",worker_index, " task_index: ",task_index)
		fmt.Println(err0)
		return false
	}
	// Execute command:
	var my_buf bytes.Buffer
	my_buf.Write([]byte(fmt.Sprintf("%d,%d", task_index, batches_count)))
	c.Stdin = &my_buf
	if err := c.Start(); err != nil {
		log.Fatal(err)
		fmt.Println("(x) MPOL ERROR: Starting external command failed! worker_index: ",worker_index, " task_index: ",task_index)
		fmt.Println(err)
		return false
	}

	// Read the result of the command:
	rr, err2 := ioutil.ReadAll(stdout)
	fmt.Println(string(rr))
	if err2 != nil {
		fmt.Println("(x) MPOL ERROR: Reading from stdout of external command failed! worker_index: ",worker_index, " task_index: ",task_index)
		fmt.Println(err2)
	}
	fmt.Println("output len: ",len(rr))
	return true

}

func spawn_worker(worker_index int, tasks_chan chan MLTask, wg *sync.WaitGroup){
	defer wg.Done()
	fmt.Println("Hello! worker: ",worker_index)
	for {
		select {
		case task := <- tasks_chan:
			task_index, batches_count := task.startIndex, task.batchesCount
			fmt.Printf("[%d] Starting task ( %d - %d )",worker_index, task_index,task_index + batches_count)			
			success := spawn_pytorch(worker_index, task)
			if success {
				fmt.Printf("[%d] Finished task (%d)\n", worker_index, task_index)
			} else{
				fmt.Printf("[%d] Failed at task (%d)\n", worker_index, task_index)
			}
			break
			
		case <- time.After(10 * time.Second):
			fmt.Printf("(%d) FINISH: Worker stopped due to empty in queue for 10 seconds.\n", worker_index)
			fmt.Println("Bye! From worker: ",worker_index)
			return
		}
	}
		
}

func generate_tasks(starting_index int, batches_count int, workers_count int,total_count int, tasks_chan chan MLTask){
	
	task_indx := starting_index
	for {		
		if task_indx + batches_count < total_count {
			fmt.Println("Puting task, in: ",task_indx)
			task := MLTask{startIndex : task_indx, batchesCount : batches_count}		
			tasks_chan <- task
			fmt.Printf("Put samples (%d to %d) in queue.\n",task_indx,task_indx + batches_count)
		} else {
			//task_indx = task_indx - batches_count
			remaining_batches := total_count - task_indx
			task := MLTask{startIndex : task_indx, batchesCount : remaining_batches}
			fmt.Println("Puting task, in: ",task_indx)
			tasks_chan <- task
			fmt.Printf("Put samples (%d to %d) in queue.\n",task_indx,task_indx + remaining_batches)
			// this was the last batch. So quit this loop.
			break
		}
		task_indx += batches_count		
	}
	fmt.Println("Finished loading tasks to queue.")
}

func call_workers(batches_count int, workers_count int, tasks_chan chan MLTask, wg *sync.WaitGroup) {

	for worker_idx := 0 ; worker_idx < workers_count ; worker_idx++ {
		wg.Add(1)
		go spawn_worker(worker_idx, tasks_chan, wg)		
	}
	fmt.Println("All workers online.")
	
}

func main() {	
	var wg sync.WaitGroup
	scanner := bufio.NewScanner(os.Stdin)
	current_page := main_menu
	stop_loop := false
	for {
		if stop_loop {
			break
		}
		switch current_page {
		case main_menu:
			{
				fmt.Println("1) Start")
				fmt.Println("2) Exit")
				scanner.Scan()
				text := scanner.Text()
				switch text {
					case "1":
						current_page = settings
						break
					case "2":
						os.Exit(2)
				}	
			}
			break
		case settings:
			{
				fmt.Println("Enter starting index:")				
				scanner.Scan()
				text := scanner.Text()
				starting_index,err := strconv.Atoi(text)
				if err != nil {
					fmt.Println("(x) MPO-Error: Invalid number. Try again.")
					break
				}
				fmt.Println("Enter batches count:")
				scanner.Scan()
				text = scanner.Text()
				batches_count, err := strconv.Atoi(text)
				if err != nil {
					fmt.Println("(x) MPO-Error: Invalid batches count number. Try again.")
					break
				}
				fmt.Println("Enter number of workers:")
				scanner.Scan()
				text = scanner.Text()
				workers_count, err := strconv.Atoi(text)
				if err != nil {
					fmt.Println("(x) MPO-Error: Invalid workers count number. Try again.")
					break
				}
				tasks_chan := make(chan MLTask, workers_count)				
				
				total_count := 10000	// test set of MNIST										

				fmt.Println(starting_index,batches_count,workers_count)
				fmt.Println("Calling in the workers...")				
				call_workers(batches_count, workers_count, tasks_chan, &wg)				
				generate_tasks(starting_index,batches_count,workers_count,total_count,tasks_chan)				
				wg.Wait()
				stop_loop = true
							
			}				
		}
		
	}
	
	
	fmt.Println("It is all finished.")
}