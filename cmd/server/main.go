package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/palmdalian/transcoder"
	"github.com/palmdalian/transcoder/cmd/server/controller"

	"github.com/gorilla/mux"
)

const (
	WorkerNum = 2
	Port      = 3210
	QueueName = "jobs"
)

func main() {
	jobQueue := make(chan *transcoder.Job, 100)
	jobUpdatesChan := make(chan *transcoder.JobStatus, 100)
	for i := 0; i < WorkerNum; i++ {
		worker := transcoder.NewWorker(jobQueue, jobUpdatesChan)
		worker.Name = fmt.Sprintf("Worker%d", i)
	}

	controller := controller.NewController(jobQueue, jobUpdatesChan)
	r := mux.NewRouter()
	r.HandleFunc("/preset-groups/{presetGroupID}/submit", controller.SubmitPresetGroupJob)
	r.HandleFunc("/presets/{presetID}/submit", controller.SubmitPresetJob)
	r.HandleFunc("/jobs", controller.GetJobs)
	r.HandleFunc("/jobs/{jobID}", controller.GetJob)
	r.HandleFunc("/jobs/{jobID}/resubmit", controller.JobResubmit)
	r.HandleFunc("/jobs/{jobID}/info", controller.JobInfo)
	r.HandleFunc("/jobs/{jobID}/kill", controller.JobKill)
	http.Handle("/", r)

	log.Printf("Listening on :%d...\n", Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Port), http.DefaultServeMux))
}
