package transcoder

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type Worker struct {
	Name           string
	jobUpdatesChan chan *Job // Channel for controller to handle any job updates
	redisClient    redis.UniversalClient
}

func NewWorker(redisClient redis.UniversalClient, jobUpdatesChan chan *Job) *Worker {
	return &Worker{
		jobUpdatesChan: jobUpdatesChan,
		redisClient:    redisClient,
	}
}

// Consume - implement rmq interface to receive jobs from redis queue
func (worker *Worker) Consume(delivery rmq.Delivery) {
	job := &Job{}
	err := json.Unmarshal([]byte(delivery.Payload()), job)
	if err != nil {
		log.Printf("Err unmarshaling delivery %v", err)
		worker.reject(job, delivery)
		return
	}

	if job.Preset == nil {
		log.Printf("Err job %v does not have a preset", job.ID)
		worker.reject(job, delivery)
		return
	}

	job.Status = JobStatusInProgress
	worker.jobUpdatesChan <- job

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	log.Printf("%s got job %s", worker.Name, job.ID)
	if err = worker.Submit(ctx, job); err != nil {
		log.Printf("Err submitting job %v", err)
		worker.reject(job, delivery)
		return
	}

	job.Status = JobStatusDone
	worker.jobUpdatesChan <- job

	// Everything completed
	if err := delivery.Ack(); err != nil {
		log.Printf("Err Acking delivery %v", err)
	}
}

func (worker *Worker) Submit(ctx context.Context, job *Job) error {
	job.Prepare()
	go worker.commandReader(ctx, job)
	if err := job.Run(); err != nil {
		return err
	}
	return nil
}

func (worker *Worker) reject(job *Job, delivery rmq.Delivery) {
	if job.ID != uuid.Nil {
		job.Status = JobStatusFailed
		worker.jobUpdatesChan <- job
	}

	if err := delivery.Reject(); err != nil {
		log.Printf("Err rejecting delivery after submit %v", err)
	}
}

// CommandChannel - Redis PubSub channel sending commands to a job
func CommandChannel(jobID uuid.UUID) string {
	return fmt.Sprintf("cmd_%v", jobID)
}

// InfoChannel - Redis PubSub channel for receiving info from a job
func InfoChannel(jobID uuid.UUID) string {
	return fmt.Sprintf("info_%v", jobID)
}

// commandReader Subscribe to a job-specific PubSub to relay commands from any pod
func (worker *Worker) commandReader(ctx context.Context, job *Job) {
	commandSub := worker.redisClient.Subscribe(ctx, CommandChannel(job.ID))
	go func() {
		<-ctx.Done()
		if err := commandSub.Close(); err != nil {
			log.Printf("Err closing send channel %v", err)
		}
	}()

	channel := commandSub.Channel()
	for message := range channel {
		switch message.Payload {
		case JobCmdStatus:
			send := worker.redisClient.Publish(ctx, InfoChannel(job.ID), job.Info())
			if err := send.Err(); err != nil {
				log.Println(err)
			}
		case JobCmdKill:
			stat := &JobStatus{Status: "killed"}
			log.Printf("Killing %v", job.ID)
			if err := job.Kill(); err != nil {
				stat.Status = job.Status
				stat.Message = fmt.Sprintf("Could not kill %v", err)
			}
			b, err := json.Marshal(stat)
			if err != nil {
				log.Println(err)
				return
			}
			send := worker.redisClient.Publish(ctx, InfoChannel(job.ID), b)
			if err := send.Err(); err != nil {
				log.Println(err)
			}
		}
	}
	fmt.Println("Exiting", job.ID)
}
