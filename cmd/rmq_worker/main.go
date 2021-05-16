package main

import (
	"log"

	"github.com/palmdalian/transcoder/queue"

	"github.com/go-redis/redis/v8"
)

const (
	WorkerNum = 2
	QueueName = "jobs"
)

func main() {
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})

	if _, err := queue.NewDirector(QueueName, WorkerNum, redisClient, nil); err != nil {
		log.Fatalf("Could not create director %v", err)
	}
	log.Println("Listening for jobs...")
	select {}
}
