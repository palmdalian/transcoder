package transcoder

import (
	"bufio"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	JobStatusSubmitted  = "submitted"
	JobStatusInProgress = "inProgress"
	JobStatusDone       = "done"
	JobStatusFailed     = "failed"
)

// JobParams - Custom map[string]string for postgres jsob compatibility
type JobParams map[string]string

type Job struct {
	ID            uuid.UUID `json:"id" gorm:"type:uuid;index"`
	CreatedAt     time.Time `json:"createdAt"`
	Status        string    `json:"status" gorm:"index"`
	PresetID      uuid.UUID `json:"presetId"`
	Preset        *Preset   `json:"preset,omitempty" gorm:"-"`
	Params        JobParams `json:"params" gorm:"type:jsonb"`
	CommandOutput string    `json:"commandOutput" gorm:"type:text"`

	mu   sync.RWMutex
	done chan struct{}
	err  error
	info *info
	cmd  *exec.Cmd
}

// NewJob - create new job with filled defaults
func NewJob(preset *Preset, params JobParams) *Job {
	return &Job{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		Status:    JobStatusSubmitted,
		PresetID:  preset.ID,
		Preset:    preset,
		Params:    params,
		info:      &info{},
	}
}

type info struct {
	CurrentTime   float64  `json:"currentTime"`
	TotalDuration float64  `json:"totalDuration"`
	Output        []string `json:"output"`
	ErrOutput     []string `json:"errOutput"`
}

type JobStatus struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Job     *Job   `json:"job"`
}

var mustacheReg = regexp.MustCompile(`^{{(\S+)}}`)

// prepare - Replace placeholders with job params
// Create exec.Cmd and attach new job.info
func (job *Job) prepare() {
	args := make([]string, len(job.Preset.Args))
	// Replace Preset placeholders with job params
	for i, arg := range job.Preset.Args {
		vals := mustacheReg.FindStringSubmatch(arg)
		if len(vals) < 2 {
			args[i] = arg
			continue
		}
		if preset, ok := job.Params[vals[1]]; ok {
			args[i] = preset
		}
	}
	job.mu.Lock()
	if job.info == nil {
		job.info = &info{}
	}
	job.cmd = exec.Command(job.Preset.Path, args...)
	job.mu.Unlock()
}

// Run - execute job cmd and collect output
// will block until job has exited
func (job *Job) Run() error {
	job.prepare()

	defer func() {
		setDone(job)
		job.CommandOutput = strings.Join(job.Output(), "\n")
	}()

	stdReader, err := job.cmd.StdoutPipe()
	if err != nil {
		job.err = err
		return err
	}
	scanner := bufio.NewScanner(stdReader)
	go job.readStdOutput(scanner)

	errReader, err := job.cmd.StderrPipe()
	if err != nil {
		job.err = err
		return err
	}
	errScanner := bufio.NewScanner(errReader)
	go job.readErrOutput(errScanner)

	job.mu.Lock()
	err = job.cmd.Start()
	job.mu.Unlock()
	if err != nil {
		job.err = err
		return err
	}

	err = job.cmd.Wait()
	if err != nil {
		job.err = err
		return err
	}

	job.Status = JobStatusDone
	return nil
}

// Reset - reset job to pre-run state
func (job *Job) Reset() {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.Status = JobStatusSubmitted
	job.CommandOutput = ""
	if job.done != nil {
		close(job.done)
	}
	job.info = &info{}
}

// setDone - close done channel if exists
func setDone(job *Job) {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.done != nil {
		close(job.done)
	}
}

// Done - channel that blocks until process has finished
func (job *Job) Done() <-chan struct{} {
	job.mu.Lock()
	if job.done == nil {
		job.done = make(chan struct{})
	}
	d := job.done
	job.mu.Unlock()
	return d
}

// Wait - block until job has finished
func (job *Job) Wait() {
	<-job.Done()
}

// Err - exec related errors. nil until process exits
func (job *Job) Err() error {
	job.mu.Lock()
	err := job.err
	job.mu.Unlock()
	return err
}

// Kill a running process
func (job *Job) Kill() error {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.cmd == nil || job.cmd.Process == nil {
		return fmt.Errorf("no job to kill %v", job.cmd)
	}
	err := job.cmd.Process.Signal(os.Kill)
	if err != nil {
		return err
	}
	job.Status = JobStatusFailed
	return nil
}

var microsecondReg = regexp.MustCompile(`^out_time_ms=(\d+)$`)

func (job *Job) readStdOutput(scanner *bufio.Scanner) {
	for scanner.Scan() {
		text := scanner.Text()
		job.appendOutput(text)
		vals := microsecondReg.FindStringSubmatch(text)
		if len(vals) < 2 {
			continue
		}
		if ms, err := strconv.ParseInt(vals[1], 10, 64); err == nil {
			job.setCurrentTime(float64(ms) / 1000000)
		}
	}
}

var timecodeReg = regexp.MustCompile(`^\s*Duration:\s*([0-9:.]+),`)

func (job *Job) readErrOutput(scanner *bufio.Scanner) {
	for scanner.Scan() {
		text := scanner.Text()
		job.appendErrOutput(text)
		vals := timecodeReg.FindStringSubmatch(text)
		if len(vals) < 2 {
			continue
		}
		dur := parseDurationFromTimecode(vals[1])
		job.setTotalDuration(dur)
	}
}

// parseDurationFromTimecode - take ffmpeg duration timecode and convert to seconds
func parseDurationFromTimecode(timecode string) float64 {
	// 00:02:15.77
	split := strings.Split(timecode, ":")
	if len(split) != 3 {
		return 0
	}
	hours, err := strconv.ParseFloat(split[0], 64)
	if err != nil {
		return 0
	}
	minutes, err := strconv.ParseFloat(split[1], 64)
	if err != nil {
		return 0
	}
	seconds, err := strconv.ParseFloat(split[2], 64)
	if err != nil {
		return 0
	}

	totalSeconds := (hours * 60 * 60) + (minutes * 60) + seconds
	return totalSeconds
}

func (job *Job) appendOutput(output string) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.info.Output = append(job.info.Output, output)
}

func (job *Job) appendErrOutput(output string) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.info.ErrOutput = append(job.info.ErrOutput, output)
}

func (job *Job) setTotalDuration(duration float64) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.info.TotalDuration = duration
}

func (job *Job) setCurrentTime(current float64) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.info.CurrentTime = current
}

// Info - return all collected info from exec.Cmd process
func (job *Job) Info() string {
	job.mu.RLock()
	defer job.mu.RUnlock()
	b, err := json.Marshal(job.info)
	if err != nil {
		return ""
	}
	return string(b)
}

// Output - return any messages collected from stdin
func (job *Job) Output() []string {
	job.mu.RLock()
	o := job.info.Output
	job.mu.RUnlock()
	return o
}

// ErrOutput - return any messages collected from stderr
func (job *Job) ErrOutput() []string {
	job.mu.RLock()
	e := job.info.ErrOutput
	job.mu.RUnlock()
	return e
}

// Scan - allow retrieving of jsonb -> JobParams
func (jp *JobParams) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := JobParams{}
	err := json.Unmarshal(bytes, &result)
	*jp = result
	return err
}

// Value - allow saving JobParams as jsonb
func (jp JobParams) Value() (driver.Value, error) {
	if len(jp) == 0 {
		return nil, nil
	}
	return json.Marshal(jp)
}
