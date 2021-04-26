package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/palmdalian/transcoder"
	"gorm.io/gorm"
)

func (c *Controller) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job := &transcoder.Job{}
	if err := c.db.Where("id = ?", jobID).First(job).Error; err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("getting job %v", err))
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

	jobs := []*transcoder.Job{}
	if err := c.db.Where("status in (?)", states).Find(&jobs).Error; err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("getting jobs %v", err))
	}
	writeJSONResponse(w, http.StatusOK, jobs)
}

func (c *Controller) JobInfo(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job := &transcoder.Job{}
	err = c.db.Where("id = ?", jobID).First(job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("jobID %v", jobID))
		return
	} else if err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("getting job %v", err))
		return
	}

	if job.Status != transcoder.JobStatusInProgress {
		stat := &transcoder.JobStatus{Status: job.Status, Message: "Job is not running"}
		writeJSONResponse(w, http.StatusBadRequest, stat)
		return
	}

	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second*3))
	defer cancel()
	msg, err := c.sendCommand(ctx, jobID, transcoder.JobCmdStatus)
	if err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("sending command %v", err))
		return
	}
	writeJSONResponse(w, http.StatusOK, msg)
}

func (c *Controller) JobKill(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job := &transcoder.Job{}
	err = c.db.Where("id = ?", jobID).First(job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("jobID %v", jobID))
		return
	} else if err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("getting job %v", err))
		return
	}

	if job.Status != transcoder.JobStatusInProgress {
		stat := &transcoder.JobStatus{Status: job.Status, Message: "Job is not running"}
		writeJSONResponse(w, http.StatusBadRequest, stat)
		return
	}

	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second*3))
	defer cancel()
	msg, err := c.sendCommand(ctx, jobID, transcoder.JobCmdKill)
	if err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("sending command %v", err))
	}
	writeJSONResponse(w, http.StatusOK, msg)
}
