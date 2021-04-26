package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/palmdalian/transcoder"
	"gorm.io/gorm"
)

type Controller struct {
	db             *gorm.DB
	redisClient    redis.UniversalClient
	taskQueue      rmq.Queue
	JobUpdatesChan chan *transcoder.Job
}

func NewController(db *gorm.DB, redisClient redis.UniversalClient, taskQueue rmq.Queue) *Controller {
	return &Controller{
		db:             db,
		redisClient:    redisClient,
		taskQueue:      taskQueue,
		JobUpdatesChan: make(chan *transcoder.Job, 10),
	}
}

func (c *Controller) SaveWorkerJobUpdates() {
	for job := range c.JobUpdatesChan {
		if job.ID == uuid.Nil {
			continue
		}
		err := c.db.Save(job).Error
		if err != nil {
			log.Printf("Err saving jobID %v: %v", job.ID, err)
		}
	}
}

func (c *Controller) DestroyQueue(w http.ResponseWriter, r *http.Request) {
	ready, rejected, err := c.taskQueue.Destroy()
	fmt.Fprintf(w, "Ready %d, Rejected %d, Err %v", ready, rejected, err)
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
	receive := c.redisClient.Subscribe(ctx, transcoder.InfoChannel(jobID))
	defer receive.Close()
	err := c.redisClient.Publish(ctx, transcoder.CommandChannel(jobID), command).Err()
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
	taskBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("could not marshal job %w", err)
	}

	err = c.taskQueue.PublishBytes(taskBytes)
	if err != nil {
		return fmt.Errorf("could not open publish queue %w", err)
	}
	log.Printf("Submitted %v", job.ID)
	return nil
}
