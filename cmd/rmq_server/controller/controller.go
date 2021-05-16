package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/palmdalian/transcoder"
	"github.com/palmdalian/transcoder/queue"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Controller struct {
	db             *gorm.DB
	redisClient    redis.UniversalClient
	director       *queue.Director
	jobUpdatesChan chan *transcoder.JobStatus
}

func NewController(db *gorm.DB, redisClient redis.UniversalClient, director *queue.Director, jobUpdatesChan chan *transcoder.JobStatus) *Controller {
	controller := &Controller{
		db:             db,
		redisClient:    redisClient,
		director:       director,
		jobUpdatesChan: jobUpdatesChan,
	}
	go controller.saveWorkerJobUpdates()
	return controller
}

func (c *Controller) PurgeReady(w http.ResponseWriter, r *http.Request) {
	ready, err := c.director.PurgeReady()
	fmt.Fprintf(w, "Ready %d, Err %v", ready, err)
}

func (c *Controller) DestroyQueue(w http.ResponseWriter, r *http.Request) {
	ready, rejected, err := c.director.DestroyQueue()
	fmt.Fprintf(w, "Ready %d, Rejected %d, Err %v", ready, rejected, err)
}

func (c *Controller) saveWorkerJobUpdates() {
	for jobStatus := range c.jobUpdatesChan {
		job := jobStatus.Job
		if job == nil {
			continue
		}
		if job.ID == uuid.Nil {
			continue
		}
		err := c.db.Save(job).Error
		if err != nil {
			log.Printf("Err saving jobID %v: %v", job.ID, err)
		}
	}
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

func (c *Controller) sendCommand(ctx context.Context, jobID uuid.UUID, command string) (string, error) {
	receive := c.redisClient.Subscribe(ctx, queue.InfoChannel(jobID))
	defer receive.Close()
	err := c.redisClient.Publish(ctx, queue.CommandChannel(jobID), command).Err()
	if err != nil {
		return "", fmt.Errorf("%v to %v: %w", command, jobID, err)
	}
	msg, err := receive.ReceiveMessage(ctx)
	if err != nil {
		return "", fmt.Errorf("%v to %v: %w", command, jobID, err)
	}
	return msg.Payload, nil
}

func (c *Controller) sendToQueue(job *transcoder.Job) error {
	err := c.db.Save(job).Error
	if err != nil {
		return fmt.Errorf("could not save job %w", err)
	}
	err = c.director.SendToQueue(job)
	if err != nil {
		return fmt.Errorf("could not open publish queue %w", err)
	}
	log.Printf("Submitted %v", job.ID)
	return nil
}
