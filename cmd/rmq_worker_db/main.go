package main

import (
	"fmt"
	"log"
	"os/user"

	"github.com/palmdalian/transcoder"
	"github.com/palmdalian/transcoder/queue"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	WorkerNum = 2
	QueueName = "jobs"
)

func main() {
	db, err := setupDB()
	if err != nil {
		log.Fatalf("Failed to setup gorm connection %v", err)
	}

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})

	jobUpdatesChan := make(chan *transcoder.JobStatus, 10)
	_, err = queue.NewDirector(QueueName, WorkerNum, redisClient, jobUpdatesChan)
	if err != nil {
		log.Fatalf("Could not create director %v", err)
	}
	log.Println("Listening for jobs...")
	go saveWorkerJobUpdates(db, jobUpdatesChan)
	select {}
}

func setupDB() (*gorm.DB, error) {
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("getting current user: %v", err)
	}
	dsn := fmt.Sprintf("host=localhost user=%s dbname=transcoder port=5432 sslmode=disable", u.Username)
	return gorm.Open(postgres.Open(dsn), &gorm.Config{})
}

func saveWorkerJobUpdates(db *gorm.DB, jobUpdatesChan chan *transcoder.JobStatus) {
	for jobStatus := range jobUpdatesChan {
		job := jobStatus.Job
		if job == nil {
			continue
		}
		if job.ID == uuid.Nil {
			continue
		}
		err := db.Save(job).Error
		if err != nil {
			log.Printf("Err saving jobID %v: %v", job.ID, err)
		}
	}
}
