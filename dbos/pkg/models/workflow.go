package models

import "time"

type Workflow struct {
	WorkflowUUID       string `json:"workflow_uuid"`
	Status             string `json:"status"`
	Name               string `json:"name"`
	Inputs             string `json:"inputs"`
	Output             string `json:"output"`
	Queue              string `json:"queue"`
	Serialization      string `json:"serialization"`
	RateLimited        bool   `json:"rate_limited"`
	ApplicationVersion string `json:"application_version"`
}

type WorkflowStepWithLevel struct {
	WorkflowUUID string     `json:"workflow_uuid"`
	FunctionName string     `json:"function_name"`
	StepName     string     `json:"step_name"`
	GlobalLevel  int        `json:"global_level"`
	LocalLevel   int        `json:"local_level"`
	Status       *string    `json:"status"`
	Output       *string    `json:"output"`
	Inputs       string     `json:"inputs"`
	StartedAt    *time.Time `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at"`
}

type WorkflowNode struct {
	Node     string   `json:"node"`
	Children []string `json:"children"`
	Disabled bool     `json:"disabled"`
	Failed   bool     `json:"failed"`
}
