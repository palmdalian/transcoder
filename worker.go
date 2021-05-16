package transcoder

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
)

type Worker struct {
	Name           string
	jobQueue       chan *Job
	jobUpdatesChan chan *JobStatus // Channel for controller to handle any job updates
}

// NewWorker create new worker and start consuming jobQueue
func NewWorker(jobQueue chan *Job, jobUpdatesChan chan *JobStatus) *Worker {
	worker := &Worker{
		Name:           fmt.Sprintf("Worker%v", uuid.NewString()[:4]),
		jobQueue:       jobQueue,
		jobUpdatesChan: jobUpdatesChan,
	}
	go worker.start()

	return worker
}

func (worker *Worker) start() {
	for job := range worker.jobQueue {
		if job.Preset == nil {
			worker.reject(job, fmt.Sprintf("job %v does not have a preset", job.ID))
			continue
		}

		job.Status = JobStatusInProgress
		worker.sendUpdate(&JobStatus{Job: job, Status: job.Status})

		log.Printf("%s got job %s", worker.Name, job.ID)
		if err := worker.submit(job); err != nil {
			worker.reject(job, fmt.Sprintf("submitting job %v", err))
			continue
		}

		job.Status = JobStatusDone
		worker.sendUpdate(&JobStatus{Job: job, Status: job.Status})
	}
}

func (worker *Worker) submit(job *Job) error {
	if err := job.Run(); err != nil {
		return err
	}
	return nil
}

func (worker *Worker) sendUpdate(status *JobStatus) {
	if worker.jobUpdatesChan == nil {
		return
	}

	// Encode so readers don't do anything weird with the *Job
	b, err := json.Marshal(status)
	if err != nil {
		log.Printf("Problem marshaling status %v", err)
		return
	}
	toSend := &JobStatus{}
	if err = json.Unmarshal(b, toSend); err != nil {
		log.Printf("Problem unmarshaling status %v", err)
	}

	worker.jobUpdatesChan <- toSend
}

func (worker *Worker) reject(job *Job, msg string) {
	job.Status = JobStatusFailed
	worker.sendUpdate(&JobStatus{Job: job, Status: job.Status, Message: msg})
}
