package transcoder

import "github.com/google/uuid"

type PresetGroup struct {
	Presets []*Preset
}

// Preset - A single command + args to exec
type Preset struct {
	Description   string     `json:"description"`
	Path          string     `json:"path"` // Executable path
	PresetGroupID *uuid.UUID `json:"presetGroupId,omitempty"`

	// Arguments to pass to executable
	// Any arguments that should be replaced by job Params should be delimited by "{{" and "}}"
	// Example: {{input}} will get replaced if "input" is present in job.Params map
	Args []string `json:"args"`
}
