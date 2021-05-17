package controller

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"

	"github.com/palmdalian/transcoder"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func (c *Controller) GetJobView(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(mux.Vars(r)["jobID"])
	if err != nil {
		writeHTMLResponse(w, http.StatusBadRequest, fmt.Sprintf("bad jobID %v", err))
		return
	}

	job, ok := c.getJob(jobID)
	if !ok {
		writeHTMLResponse(w, http.StatusNotFound, fmt.Sprintf("getting job %v", err))
		return
	}
	t, err := template.New("").Funcs(fm).Parse(jobTemplate)
	if err != nil {
		writeHTMLResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error parsing template %v", err))
	}
	tmp := &templateFullJob{}
	if err := json.Unmarshal([]byte(job.Info()), tmp); err != nil {
		writeHTMLResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error marshaling info %v", err))
	}
	tmp.Job = job
	t.Execute(w, tmp)
}

func (c *Controller) GetJobsView(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	states := params["status"]
	if len(states) == 0 {
		states = []string{transcoder.JobStatusSubmitted, transcoder.JobStatusInProgress}
	}

	jobs := c.getJobs(states)
	t, err := template.New("").Parse(jobsTemplate)
	if err != nil {
		writeHTMLResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error executing template %v", err))
	}
	t.Execute(w, jobs)
}

var fm = template.FuncMap{"percent": func(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return math.Round(a / b * 100)
}}

type templateFullJob struct {
	Job           *transcoder.Job
	ID            uuid.UUID
	Status        string
	Preset        transcoder.Preset
	Params        transcoder.JobParams
	TotalDuration float64
	CurrentTime   float64
	ErrOutput     []string
	Output        []string
}

var jobTemplate = `
	<h1>ID: {{.Job.ID}}</h1><p>
	Status: {{.Job.Status}}<br>
	Percent complete: {{percent .CurrentTime .TotalDuration }} % <br>
	Created: {{.Job.CreatedAt}}<br>
	Preset: {{.Job.Preset}}<br>
	Params: {{.Job.Params}}<br>
	<p>
	StdErr:<br>
	<div style="background: ghostwhite; 
            font-size: 20px; 
            padding: 10px;
			height: 200px;
			overflow-y:auto;
            border: 1px solid lightgray; 
            margin: 10px;">
	{{- range .ErrOutput }}
	{{.}}<br>
	{{- end}}
	</div>
	StdOut:<br>
	<div style="background: ghostwhite; 
	font-size: 20px; 
	padding: 10px; 
	height: 400px;
	overflow-y:auto;
	border: 1px solid lightgray; 
	margin: 10px;">
	{{- range .Output }}<br>
	{{.}}
	{{- end}}
	</div>
`

var jobsTemplate = `
<html>
<head>
<script type="text/javascript">
    function selectState(){
		var url = window.location.protocol + '//' + window.location.host + window.location.pathname
        var states = document.getElementById('states');
        var selectedState = states.options[states.selectedIndex];
		if (selectedState.value != ""){
        window.location = url + "?status=" + selectedState.value;
		} else{
			window.location = url;
		}
    }
</script>
</head>
<body>
<label for="states">View jobs by state:</label>
<select id="states" name="states" onchange="selectState()" onfocus="this.selectedIndex = -1;">
	<option value=""></option>
  <option value="inProgress">In Progress</option>
  <option value="submitted">Submitted</option>
  <option value="done">Done</option>
  <option value="failed">Failed</option>
</select>

{{- range . }}
<a href="jobs/{{.ID}}/info.html">
<div style="background: ghostwhite; 
padding: 10px;
border: 2px solid lightgray; 
margin: 5px;">
	<h1>Job {{.ID}} - {{.Status}}</h1>
	Created: {{.CreatedAt}}<br>
	Preset: {{.Preset}}<br>
	Params: {{.Params}}<br>
</div>
</a>
{{- end }}
</body>
</html>
`
