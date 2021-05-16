package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/palmdalian/transcoder"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// Director connects to rmq.Queue to submit, consume, reject, and ack deliveries
// Any consumed deliveries are sent to the job queue
type Director struct {
	jobQueue    chan *transcoder.Job
	taskQueue   rmq.Queue
	redisClient redis.UniversalClient
}

// NewDirector opens rmq.Queue and starts worker pool to run jobs
// Director both Consumes and Submits rmq deliveries
func NewDirector(queueName string, workerNum int, redisClient redis.UniversalClient, jobUpdatesChan chan *transcoder.JobStatus) (*Director, error) {
	rClient, ok := redisClient.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("could not assert redis client")
	}

	errChan := make(chan error)
	defer close(errChan)
	go logErrors(errChan)
	connection, err := rmq.OpenConnectionWithRedisClient(uuid.NewString(), rClient, errChan)
	if err != nil {
		return nil, fmt.Errorf("could not open rmq connection %w", err)
	}

	taskQueue, err := connection.OpenQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("could not open rmq queue %w", err)
	}

	go startCleaner(connection)
	go startPurger(taskQueue)

	err = taskQueue.StartConsuming(int64(workerNum), time.Second)
	if err != nil {
		log.Fatalf("Could not start consuming %v", err)
	}

	jobQueue := make(chan *transcoder.Job, 100)
	director := &Director{
		jobQueue:    jobQueue,
		taskQueue:   taskQueue,
		redisClient: redisClient,
	}

	for i := 0; i < workerNum; i++ {
		if _, err := taskQueue.AddConsumer(fmt.Sprintf("consumer-%d", i), director); err != nil {
			return nil, err
		}
		worker := transcoder.NewWorker(jobQueue, jobUpdatesChan)
		worker.Name = fmt.Sprintf("Worker%d", i)
	}

	return director, nil
}

// Consume - implement rmq interface to receive jobs from redis queue
func (director *Director) Consume(delivery rmq.Delivery) {
	job := &transcoder.Job{}
	err := json.Unmarshal([]byte(delivery.Payload()), job)
	if err != nil {
		log.Printf("Err unmarshaling delivery %v", err)
		reject(delivery)
		return
	}

	if job.Preset == nil {
		log.Printf("Err job %v does not have a preset", job.ID)
		reject(delivery)
		return
	}
	job.Reset()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go director.commandReader(ctx, job)

	director.jobQueue <- job
	job.Wait()
	if job.Err() != nil {
		reject(delivery)
		return
	}

	// Everything completed
	if err = delivery.Ack(); err != nil {
		log.Printf("Err Acking delivery %v", err)
	}
}

// SendToQueue send new job to rmq.Queue to be picked up by any listening directors
func (director *Director) SendToQueue(job *transcoder.Job) error {
	taskBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("could not marshal job %w", err)
	}

	err = director.taskQueue.PublishBytes(taskBytes)
	if err != nil {
		return fmt.Errorf("could not open publish queue %w", err)
	}
	return nil
}

const (
	jobCmdStatus = "status"
	jobCmdKill   = "kill"
)

// KillJob - use pubsub to send kill command to attached director for running jobID
func (director *Director) KillJob(ctx context.Context, jobID uuid.UUID) (string, error) {
	return director.sendCommand(ctx, jobID, jobCmdKill)
}

// JobInfo - use pubsub to get job info from the director of the running jobID
func (director *Director) JobInfo(ctx context.Context, jobID uuid.UUID) (string, error) {
	return director.sendCommand(ctx, jobID, jobCmdStatus)
}

func (director *Director) sendCommand(ctx context.Context, jobID uuid.UUID, command string) (string, error) {
	receive := director.redisClient.Subscribe(ctx, infoChannel(jobID))
	defer receive.Close()
	err := director.redisClient.Publish(ctx, commandChannel(jobID), command).Err()
	if err != nil {
		return "", fmt.Errorf("%v to %v: %w", command, jobID, err)
	}
	msg, err := receive.ReceiveMessage(ctx)
	if err != nil {
		return "", fmt.Errorf("%v to %v: %w", command, jobID, err)
	}
	return msg.Payload, nil
}

// PurgeReady remove any unAcked jobs before work is started
func (director *Director) PurgeReady() (ready int64, err error) {
	return director.taskQueue.PurgeReady()
}

// DestroyQueue remove any running or queued jobs
func (director *Director) DestroyQueue() (ready, rejected int64, err error) {
	return director.taskQueue.Destroy()
}

// commandReader Subscribe to a job-specific PubSub to relay commands from any pod
func (director *Director) commandReader(ctx context.Context, job *transcoder.Job) {
	commandSub := director.redisClient.Subscribe(ctx, commandChannel(job.ID))
	go func() {
		<-ctx.Done()
		if err := commandSub.Close(); err != nil {
			log.Printf("Err closing send channel %v", err)
		}
	}()
	defer log.Printf("Exiting reader for job %v", job.ID)

	channel := commandSub.Channel()
	for message := range channel {
		switch message.Payload {
		case jobCmdStatus:
			send := director.redisClient.Publish(ctx, infoChannel(job.ID), job.Info())
			if err := send.Err(); err != nil {
				log.Println(err)
			}
		case jobCmdKill:
			stat := &transcoder.JobStatus{Status: "killed", Job: job}
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
			send := director.redisClient.Publish(ctx, infoChannel(job.ID), b)
			if err := send.Err(); err != nil {
				log.Println(err)
			}
			// We killed the job
			return
		}
	}
}

// commandChannel - Redis PubSub channel sending commands to a job
func commandChannel(jobID uuid.UUID) string {
	return fmt.Sprintf("cmd_%v", jobID)
}

// infoChannel - Redis PubSub channel for receiving info from a job
func infoChannel(jobID uuid.UUID) string {
	return fmt.Sprintf("info_%v", jobID)
}

// reject - rejected deliveries will not be retried
func reject(delivery rmq.Delivery) {
	if err := delivery.Reject(); err != nil {
		log.Printf("Err rejecting delivery after submit %v", err)
	}
}

// startPurger - purge rejected deliveries
func startPurger(queue rmq.Queue) {
	for range time.Tick(time.Second * 10) {
		count, err := queue.PurgeRejected()
		if err != nil {
			log.Printf("failed to purge: %v", err)
			continue
		}
		if count > 0 {
			log.Printf("purged %d", count)
		}
	}
}

// startCleaner - resets unacked deliveries from dead consumers
func startCleaner(connection rmq.Connection) {
	cleaner := rmq.NewCleaner(connection)
	for range time.Tick(time.Second) {
		returned, err := cleaner.Clean()
		if err != nil {
			log.Printf("failed to clean: %v", err)
			continue
		}
		if returned > 0 {
			log.Printf("cleaned %d", returned)
		}
	}
}

// logErrors log any rmq errors
func logErrors(errChan <-chan error) {
	for err := range errChan {
		switch err := err.(type) {
		case *rmq.HeartbeatError:
			if err.Count == rmq.HeartbeatErrorLimit {
				log.Print("heartbeat error (limit): ", err)
			} else {
				log.Print("heartbeat error: ", err)
			}
		case *rmq.ConsumeError:
			log.Print("consume error: ", err)
		case *rmq.DeliveryError:
			log.Print("delivery error: ", err.Delivery, err)
		default:
			log.Print("other error: ", err)
		}
	}
}
