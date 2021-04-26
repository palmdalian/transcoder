package transcoder

import (
	"bufio"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

const (
	JobCmdStatus = "status"
	JobCmdKill   = "kill"

	JobStatusSubmitted  = "submitted"
	JobStatusInProgress = "inProgress"
	JobStatusDone       = "done"
	JobStatusFailed     = "failed"
)

// JobParams - Custom map[string]string for postgres jsob compatibility
type JobParams map[string]string

type Job struct {
	ID            uuid.UUID      `json:"id" gorm:"type:uuid;index"`
	Status        string         `json:"status" gorm:"index"`
	Preset        *Preset        `json:"preset" gorm:"-"`
	Params        JobParams      `json:"params" gorm:"type:jsonb"`
	CommandOutput pq.StringArray `json:"commandOutput" gorm:"type:text[]"`

	info *info
	cmd  *exec.Cmd
}

type info struct {
	mutex        sync.RWMutex
	TotalFrames  int64    `json:"totalFrames"`
	CurrentFrame int64    `json:"currentFrame"`
	Output       []string `json:"output"`
	ErrOutput    []string `json:"errOutput"`
}

type JobStatus struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

var mustacheReg = regexp.MustCompile(`^{{(\S+)}}`)

func (job *Job) Prepare() {
	job.info = &info{}
	// Replace Preset placeholders with job params
	for i, arg := range job.Preset.Args {
		vals := mustacheReg.FindStringSubmatch(arg)
		if len(vals) < 2 {
			continue
		}
		if preset, ok := job.Params[vals[1]]; ok {
			job.Preset.Args[i] = preset
		}
	}
	job.cmd = exec.Command(job.Preset.Path, job.Preset.Args...)
}

func (job *Job) Run() error {
	if job.cmd == nil {
		return fmt.Errorf("job not prepared")
	}

	defer func() {
		job.CommandOutput = job.Output()
	}()

	stdReader, err := job.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdReader)
	go job.readStdOutput(scanner)

	errReader, err := job.cmd.StderrPipe()
	if err != nil {
		return err
	}
	errScanner := bufio.NewScanner(errReader)
	go job.readErrOutput(errScanner)

	err = job.cmd.Start()
	if err != nil {
		return err
	}

	err = job.cmd.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (job *Job) Kill() error {
	if job.cmd != nil && job.cmd.Process != nil {
		return fmt.Errorf("no job to kill %v", job.cmd)
	}
	return job.cmd.Process.Signal(os.Kill)
}

var frameReg = regexp.MustCompile(`^frame=(\d+)$`)

func (job *Job) readStdOutput(scanner *bufio.Scanner) {
	for scanner.Scan() {
		text := scanner.Text()
		job.AppendOutput(text)
		vals := frameReg.FindStringSubmatch(text)
		if len(vals) < 2 {
			continue
		}
		if frame, err := strconv.ParseInt(vals[1], 10, 64); err == nil {
			job.SetFrame(frame)
		}
	}
}

var durationReg = regexp.MustCompile(`^\s*Duration:\s*([0-9:.]+),`)

func (job *Job) readErrOutput(scanner *bufio.Scanner) {
	for scanner.Scan() {
		text := scanner.Text()
		job.AppendErrOutput(text)
		vals := durationReg.FindStringSubmatch(text)
		if len(vals) < 2 {
			continue
		}
		frames := parseFramesFromTimecode(vals[1])
		job.SetTotalFrames(frames)
	}
}

// parseFramesFromTimecode - take ffmpeg duration timecode and convert to frames
// assumes 29.97 framerate :(
func parseFramesFromTimecode(timecode string) int64 {
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
	frames := int64(math.Round(totalSeconds * 30000 / 1001))
	return frames
}

func (job *Job) AppendOutput(output string) {
	job.info.mutex.Lock()
	defer job.info.mutex.Unlock()
	job.info.Output = append(job.info.Output, output)
}

func (job *Job) AppendErrOutput(output string) {
	job.info.mutex.Lock()
	defer job.info.mutex.Unlock()
	job.info.ErrOutput = append(job.info.ErrOutput, output)
}

func (job *Job) SetTotalFrames(frames int64) {
	job.info.mutex.Lock()
	defer job.info.mutex.Unlock()
	job.info.TotalFrames = frames
}

func (job *Job) SetFrame(frame int64) {
	job.info.mutex.Lock()
	defer job.info.mutex.Unlock()
	job.info.CurrentFrame = frame
}

func (job *Job) Info() string {
	job.info.mutex.RLock()
	defer job.info.mutex.RUnlock()
	b, err := json.Marshal(job.info)
	if err != nil {
		return ""
	}
	return string(b)
}

func (job *Job) Output() []string {
	job.info.mutex.RLock()
	defer job.info.mutex.RUnlock()
	return job.info.Output
}

func (job *Job) ErrOutput() []string {
	job.info.mutex.RLock()
	defer job.info.mutex.RUnlock()
	return job.info.ErrOutput
}

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

func (jp JobParams) Value() (driver.Value, error) {
	if len(jp) == 0 {
		return nil, nil
	}
	return json.Marshal(jp)
}
