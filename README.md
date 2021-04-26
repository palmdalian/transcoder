# transcoder
Golang video transcode workers to run FFMPEG commands. Created with multiple server instances in mind. 

Uses rmq (Redis message queue) for the job queue:
https://github.com/adjust/rmq

Example server in cmd/server