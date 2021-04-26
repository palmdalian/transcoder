package controller

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/palmdalian/transcoder"
)

type JobSubmission struct {
	Params map[string]string `json:"params"`
}

// Realistically, these should be saved in the DB
var presets = map[uuid.UUID]*transcoder.Preset{
	uuid.MustParse("da303a92-d681-4be5-8880-668377edf37c"): {
		Description: "Convert using ffmpeg defaults",
		Path:        "ffmpeg",
		Args:        []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "{{output}}"},
	},
	uuid.MustParse("f12e777d-4666-484c-99b9-fd0ec24c9f3e"): {
		Description: "Stream copy to mp4",
		Path:        "ffmpeg",
		Args:        []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "-c", "copy", "{{output}}.mp4"},
	},
	uuid.MustParse("8826501e-bfa3-4743-b4d1-305dd1a40c72"): {
		Description: "Audio only copy",
		Path:        "ffmpeg",
		Args:        []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "-c:a", "copy", "-vn", "{{output}}"},
	},
	uuid.MustParse("2f7b5825-4ff9-4407-bf6e-20b0d2125d01"): {
		Description: "Video only copy",
		Path:        "ffmpeg",
		Args:        []string{"-y", "-progress", "-", "-nostats", "-i", "{{input}}", "-c:v", "copy", "-an", "{{output}}"},
	},
}

var presetGroups = map[uuid.UUID]*transcoder.PresetGroup{
	uuid.MustParse("3d42ee9d-dfe2-4105-b0ab-abfbcbc0d795"): {
		Presets: []*transcoder.Preset{
			presets[uuid.MustParse("da303a92-d681-4be5-8880-668377edf37c")],
			presets[uuid.MustParse("f12e777d-4666-484c-99b9-fd0ec24c9f3e")],
		},
	},
}

func (c *Controller) SubmitPresetJob(w http.ResponseWriter, r *http.Request) {
	presetID, err := uuid.Parse(mux.Vars(r)["presetID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad presetID %v", err))
		return
	}

	preset, ok := presets[presetID]
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("presetID %v", presetID))
		return
	}

	defer r.Body.Close()
	submission := &JobSubmission{}
	if err = json.NewDecoder(r.Body).Decode(submission); err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("decoding job submission %v", err))
		return
	}

	job := &transcoder.Job{
		ID:     uuid.New(),
		Status: transcoder.JobStatusSubmitted,
		Preset: preset,
		Params: submission.Params,
	}

	if err = c.sendToQueue(job); err != nil {
		writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("submitting to queue %v", err))
		return
	}
	writeJSONResponse(w, http.StatusAccepted, job)
}

func (c *Controller) SubmitPresetGroupJob(w http.ResponseWriter, r *http.Request) {
	presetGroupID, err := uuid.Parse(mux.Vars(r)["presetGroupID"])
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("bad presetGroupID %v", err))
		return
	}

	presetGroup, ok := presetGroups[presetGroupID]
	if !ok {
		writeErrResponse(w, http.StatusNotFound, fmt.Sprintf("presetGroupID %v", presetGroupID))
		return
	}

	defer r.Body.Close()
	submission := &JobSubmission{}
	err = json.NewDecoder(r.Body).Decode(submission)
	if err != nil {
		writeErrResponse(w, http.StatusBadRequest, fmt.Sprintf("decoding job submission %v", err))
		return
	}

	jobs := make([]*transcoder.Job, len(presetGroup.Presets))
	for i, preset := range presetGroup.Presets {
		job := &transcoder.Job{
			ID:     uuid.New(),
			Status: transcoder.JobStatusSubmitted,
			Preset: preset,
			Params: submission.Params,
		}
		jobs[i] = job
		if err := c.sendToQueue(job); err != nil {
			writeErrResponse(w, http.StatusInternalServerError, fmt.Sprintf("submitting to queue %v", err))
			return
		}
	}
	writeJSONResponse(w, http.StatusAccepted, jobs)
}
