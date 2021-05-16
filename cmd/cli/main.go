package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"sync"

	"github.com/palmdalian/transcoder"
)

const (
	WorkerNum = 2
)

var preset = &transcoder.Preset{
	Path: "ffmpeg",
	Args: []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "{{output}}"},
}

func main() {

	var input string
	var output string
	flag.StringVar(&input, "i", "", "input directory")
	flag.StringVar(&output, "o", "", "output directory")
	flag.Parse()

	if input == "" {
		flag.Usage()
		log.Fatalf("Input directory must be present")
	}

	if output == "" {
		flag.Usage()
		log.Fatalf("Output directory must be present")
	}

	jobQueue := make(chan *transcoder.Job, 100)
	jobUpdatesChan := make(chan *transcoder.JobStatus, 100)
	go printUpdates(jobUpdatesChan)
	for i := 0; i < WorkerNum; i++ {
		worker := transcoder.NewWorker(jobQueue, jobUpdatesChan)
		worker.Name = fmt.Sprintf("Worker%d", i)
	}

	files, err := ioutil.ReadDir(input)
	if err != nil {
		log.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		wg.Add(1)
		go submitJob(wg, jobQueue, path.Join(input, f.Name()), path.Join(output, f.Name()))
	}
	wg.Wait()
}

func submitJob(wg *sync.WaitGroup, jobQueue chan *transcoder.Job, input, output string) {
	defer wg.Done()

	preset := &transcoder.Preset{
		Path: "ffmpeg",
		Args: []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "{{output}}"},
	}
	params := map[string]string{
		"input":  input,
		"output": output,
	}

	job := transcoder.NewJob(preset, params)
	log.Printf("Submitting %v %v : %v", job.ID, input, output)
	jobQueue <- job
	job.Wait()
}

func printUpdates(jobUpdatesChan chan *transcoder.JobStatus) {
	for update := range jobUpdatesChan {
		log.Printf("%v %v Status: %s %s", update.Job.ID, update.Job.Params, update.Status, update.Message)
	}
}
