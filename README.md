# transcoder
Golang video transcode workers to run FFMPEG commands using `os/exec`. Created with multiple server instances in mind.

Optionally uses rmq (Redis message queue) as a distributed worker job queue:
https://github.com/adjust/rmq

## Creating a new job
Jobs must have a valid Preset to run. JobParams (map[string]string) can be attached before submission to replace `{{placeholder}}` strings in a Preset.
```
	preset := &transcoder.Preset{
		Path: "ffmpeg",
		Args: []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "{{output}}"},
	}

	params := map[string]string{
		"input":  "input.mp4",
		"output": "output.mp4",
	}

	job := transcoder.NewJob(preset, params)
```

## Getting jobs from an rmq.Queue
A NewDirector in the queue package creates a worker pool and subscribes to a rmq.Queue
```
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})

	jobUpdatesChan := make(chan *transcoder.JobStatus, 100)
	director, err := queue.NewDirector(QueueName, WorkerNum, redisClient, jobUpdatesChan)
	if err != nil {
		log.Fatalf("Could not create director %v", err)
	}
```

## Creating a standalone worker pool
If you don't want to use an rmq.Queue, you can instead create your own worker pool.
```
    // Queue to submit new jobs
	jobQueue := make(chan *transcoder.Job, 100)

	// Chan for status updates from workers (alternatively, can pass nil)
	jobUpdatesChan := make(chan *transcoder.JobStatus, 100)
	for i := 0; i < WorkerNum; i++ {
		transcoder.NewWorker(jobQueue, jobUpdatesChan)
	}

	// Read updates
	go func() {
		for update := range jobUpdatesChan {
			log.Printf("%v Status: %s %s", update.Job.ID, update.Status, update.Message)
		}
	}()

    ...

	// Send new jobs to the pool
	newJob := &transcoder.Job{Preset: &transcoder.Preset{}}
	jobQueue <- newJob

	// Block until job finishes
	newJob.Wait()

```


## Examples
Example servers, workers, and cli in /cmd
### Standalone http server
`cmd/server` Listen for incoming JobSubmission requests and submit them to the worker pool. Handles running, killing, and getting job status.
### CLI with pool
`cmd/cli` Serially run many jobs directly using a single Preset. flag args are used for JobParams input and output.
### CLI with pool
`cmd/cli_pool` Batch many ffmpeg jobs using a single Preset. flag args are used for JobParams input and output.
### Server with rmq.Queue and DB saves
`cmd/rmq_server` Listen for incoming JobSubmission requests and add them to rmq.Queue to via queue.Director. Saves any JobStatus messages to the db
### Worker with rmq.Queue and DB saves
`cmd/rmq_worker_db` Listen to rmq.Queue to via queue.Director and run consumed jobs. Saves any JobStatus messages to the db
### Worker with rmq.Queue
`cmd/rmq_worker` Listen to rmq.Queue to via queue.Director and run consumed jobs.
