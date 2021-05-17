package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/palmdalian/transcoder"

	"github.com/google/uuid"
)

type Controller struct {
	mutex          *sync.Mutex
	jobs           map[uuid.UUID]*transcoder.Job
	jobChan        chan *transcoder.Job
	jobUpdatesChan chan *transcoder.JobStatus
}

func NewController(jobChan chan *transcoder.Job, jobUpdatesChan chan *transcoder.JobStatus) *Controller {
	controller := &Controller{
		jobChan:        jobChan,
		jobUpdatesChan: jobUpdatesChan,
		mutex:          &sync.Mutex{},
		jobs:           make(map[uuid.UUID]*transcoder.Job),
	}
	return controller
}

func writeHTMLResponse(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/html")
	if code != http.StatusOK {
		w.WriteHeader(code)
	}
	w.Write([]byte(body))
}

func writeJSONResponse(w http.ResponseWriter, code int, body interface{}) {
	resp, err := json.Marshal(body)
	if err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("marshaling jobs %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if code != http.StatusOK {
		w.WriteHeader(code)
	}
	w.Write(resp)
}

type errResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeErrResponse(w http.ResponseWriter, code int, message string) {
	resp := errResponse{Code: code, Message: message}
	b, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(code)
		w.Write([]byte(message))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

func (c *Controller) sendToQueue(job *transcoder.Job) error {
	c.mutex.Lock()
	c.jobs[job.ID] = job
	c.mutex.Unlock()
	c.jobChan <- job
	go func() {
		job.Wait()
		log.Printf("%v err: %v", job.ID, job.Err())
	}()
	return nil
}
