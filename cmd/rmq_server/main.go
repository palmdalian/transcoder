package main

import (
	"fmt"
	"log"
	"net/http"
	"os/user"

	"github.com/palmdalian/transcoder"
	"github.com/palmdalian/transcoder/cmd/rmq_server/controller"
	"github.com/palmdalian/transcoder/queue"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	WorkerNum = 2 // Set to zero to avoid running jobs
	Port      = 3210
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
	director, err := queue.NewDirector(QueueName, WorkerNum, redisClient, jobUpdatesChan)
	if err != nil {
		log.Fatalf("Could not create director %v", err)
	}

	controller := controller.NewController(db, redisClient, director, jobUpdatesChan)

	r := mux.NewRouter()
	r.HandleFunc("/preset-groups/{presetGroupID}/submit", controller.SubmitPresetGroupJob)
	r.HandleFunc("/presets/{presetID}/submit", controller.SubmitPresetJob)
	r.HandleFunc("/jobs", controller.GetJobs)
	r.HandleFunc("/jobs/{jobID}", controller.GetJob)
	r.HandleFunc("/jobs/{jobID}/resubmit", controller.JobResubmit)
	r.HandleFunc("/jobs/{jobID}/info", controller.JobInfo)
	r.HandleFunc("/jobs/{jobID}/kill", controller.JobKill)
	r.HandleFunc("/purge-ready", controller.PurgeReady)
	r.HandleFunc("/destroy-queue", controller.DestroyQueue)
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
