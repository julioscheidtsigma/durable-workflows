package models

type Workflow struct {
	WorkflowUUID       string `json:"workflow_uuid"`
	Status             string `json:"status"`
	Name               string `json:"name"`
	Inputs             string `json:"inputs"`
	Output             string `json:"output"`
	Error              string `json:"error"`
	Queue              string `json:"queue"`
	Serialization      string `json:"serialization"`
	RateLimited        bool   `json:"rate_limited"`
	ApplicationVersion string `json:"application_version"`
}
