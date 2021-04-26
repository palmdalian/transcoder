package main

import (
	"fmt"
	"log"
	"net/http"
	"os/user"
	"time"

	"github.com/palmdalian/transcoder"
	"github.com/palmdalian/transcoder/cmd/server/controller"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	WorkerNum = 5
	Port      = 3210
	Tag       = "transcoder"
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
	errChan := make(chan error)
	go logErrors(errChan)
	rClient, ok := redisClient.(*redis.Client)
	if !ok {
		log.Fatalf("Could not assert redis client")
	}
	connection, err := rmq.OpenConnectionWithRedisClient(Tag, rClient, errChan)
	if err != nil {
		log.Fatalf("Could not open rmq connection %v", err)
	}

	taskQueue, err := connection.OpenQueue(QueueName)
	if err != nil {
		log.Fatalf("Could not open rmq queue %v", err)
	}

	go startCleaner(connection)
	go startPurger(taskQueue)

	err = taskQueue.StartConsuming(WorkerNum, time.Second)
	if err != nil {
		log.Fatalf("Could not start consuming %v", err)
	}

	con := controller.NewController(db, redisClient, taskQueue)
	go con.SaveWorkerJobUpdates()

	for i := 0; i < WorkerNum; i++ {
		worker := transcoder.NewWorker(redisClient, con.JobUpdatesChan)
		name, err := taskQueue.AddConsumer(fmt.Sprintf("worker%d", i), worker)
		worker.Name = name
		if err != nil {
			log.Fatalf("Could not add consumer %v", err)
		}
	}

	r := mux.NewRouter()
	r.HandleFunc("/preset-groups/{presetGroupID}/submit", con.SubmitPresetGroupJob)
	r.HandleFunc("/presets/{presetID}/submit", con.SubmitPresetJob)
	r.HandleFunc("/jobs", con.GetJobs)
	r.HandleFunc("/jobs/{jobID}", con.GetJob)
	r.HandleFunc("/jobs/{jobID}/info", con.JobInfo)
	r.HandleFunc("/jobs/{jobID}/kill", con.JobKill)
	r.HandleFunc("/destroy-queue", con.DestroyQueue)
	http.Handle("/", r)

	log.Printf("Listening on :%d...\n", Port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", Port), http.DefaultServeMux)
	if err != nil {
		log.Fatal(err)
	}

}

func setupDB() (*gorm.DB, error) {
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("getting current user: %v", err)
	}
	dsn := fmt.Sprintf("host=localhost user=%s dbname=transcoder port=5432 sslmode=disable", u.Username)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.Exec(`
	DO $$
	BEGIN
		CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	END$$;
	`).Error
	if err != nil {
		return nil, err
	}
	if err = db.AutoMigrate(&transcoder.Job{}); err != nil {
		return nil, err
	}

	return db, nil
}

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
