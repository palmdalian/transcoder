package controller

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/palmdalian/transcoder"
)

func (c *Controller) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job, ok := c.getJob(jobID)
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("getting job %v", err))
		return
	}
	writeJSONResponse(w, http.StatusOK, job)
}

func (c *Controller) GetJobs(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	states := params["status"]
	if len(states) == 0 {
		states = []string{transcoder.JobStatusSubmitted, transcoder.JobStatusInProgress}
	}

	jobs := c.getJobs(states)
	writeJSONResponse(w, http.StatusOK, jobs)
}

func (c *Controller) JobInfo(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job, ok := c.getJob(jobID)
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("jobID %v", jobID))
		return
	}

	if job.Status != transcoder.JobStatusInProgress {
		stat := &transcoder.JobStatus{Status: job.Status, Message: "Job is not running", Job: job}
		writeJSONResponse(w, http.StatusBadRequest, stat)
		return
	}

	writeJSONResponse(w, http.StatusOK, job.Info())
}

func (c *Controller) JobKill(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job, ok := c.getJob(jobID)
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("jobID %v", jobID))
		return
	}

	if job.Status != transcoder.JobStatusInProgress {
		stat := &transcoder.JobStatus{Status: job.Status, Message: "Job is not running", Job: job}
		writeJSONResponse(w, http.StatusBadRequest, stat)
		return
	}

	writeJSONResponse(w, http.StatusOK, job.Kill())
}

func (c *Controller) JobResubmit(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job, ok := c.getJob(jobID)
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("jobID %v", jobID))
		return
	}
	if job.Status == transcoder.JobStatusInProgress {
		stat := &transcoder.JobStatus{Status: job.Status, Message: "Job is not running", Job: job}
		writeJSONResponse(w, http.StatusBadRequest, stat)
		return
	}

	job.Reset()
	preset, ok := presets[job.PresetID]
	if !ok {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad presetID %v", job.PresetID))
	}
	job.Preset = preset

	if err = c.sendToQueue(job); err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("submitting to queue %v", err))
		return
	}
	writeJSONResponse(w, http.StatusAccepted, job)
}

func (c *Controller) getJob(jobID uuid.UUID) (*transcoder.Job, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	job, ok := c.jobs[jobID]
	return job, ok
}

func (c *Controller) getJobs(states []string) []*transcoder.Job {
	stateMap := make(map[string]bool, len(states))
	for _, s := range states {
		stateMap[s] = true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	jobs := make([]*transcoder.Job, 0, len(c.jobs))
	for _, job := range c.jobs {
		if _, ok := stateMap[job.Status]; !ok {
			continue
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.Before(jobs[j].CreatedAt) })
	return jobs
}
