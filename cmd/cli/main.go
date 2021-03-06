package main

import (
	"flag"
	"io/ioutil"
	"log"
	"path"

	"github.com/palmdalian/transcoder"
)

const (
	WorkerNum = 2
)

var preset = &transcoder.Preset{
	Path: "ffmpeg",
	Args: []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "{{output}}"},
}

func main() {

	var input string
	var output string
	flag.StringVar(&input, "i", "", "input directory")
	flag.StringVar(&output, "o", "", "output directory")
	flag.Parse()

	if input == "" {
		flag.Usage()
		log.Fatalf("Input directory must be present")
	}

	if output == "" {
		flag.Usage()
		log.Fatalf("Output directory must be present")
	}

	files, err := ioutil.ReadDir(input)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		err := runJob(path.Join(input, f.Name()), path.Join(output, f.Name()))
		if err != nil {
			log.Println(err)
		}
	}
}

func runJob(input, output string) error {
	params := map[string]string{
		"input":  input,
		"output": output,
	}

	job := transcoder.NewJob(preset, params)
	log.Printf("Running %v %v : %v", job.ID, input, output)
	err := job.Run()
	return err
}
