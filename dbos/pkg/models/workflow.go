package models

import (
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/julioscheidtsigma/dbos/api/responses"
)

type Workflow struct {
	WorkflowUUID       string  `json:"workflowUUID"`
	Status             string  `json:"status"`
	Name               string  `json:"name"`
	Inputs             string  `json:"inputs"`
	Output             *string `json:"output"`
	Attempts           int     `json:"attempts"`
	Queue              *string `json:"queue"`
	Serialization      string  `json:"serialization"`
	RateLimited        bool    `json:"rateLimited"`
	ApplicationVersion string  `json:"applicationVersion"`
}

func WorkflowFromStatus(ws dbos.WorkflowStatus) Workflow {
	input := ""
	output := ""
	if ws.Input != nil {
		input = ws.Input.(string)
	}
	if ws.Output != nil {
		output = ws.Output.(string)
	}
	return Workflow{
		WorkflowUUID:  ws.ID,
		Status:        string(ws.Status),
		Name:          ws.Name,
		Inputs:        input,
		Output:        &output,
		Attempts:      ws.Attempts,
		Queue:         &ws.QueueName,
		Serialization: ws.Serialization,
	}
}

type WorkflowStepWithLevel struct {
	WorkflowUUID string     `json:"workflowUUID"`
	FunctionName string     `json:"functionName"`
	StepName     string     `json:"stepName"`
	GlobalLevel  int        `json:"globalLevel"`
	LocalLevel   int        `json:"localLevel"`
	Status       *string    `json:"status"`
	Output       *string    `json:"output"`
	Inputs       string     `json:"inputs"`
	StartedAt    *time.Time `json:"startedAt"`
	CompletedAt  *time.Time `json:"completedAt"`
}

type WorkflowNodesWithStatus struct {
	Nodes          map[int][]WorkflowNode `json:"nodes"`
	WorkflowStatus string                 `json:"workflowStatus"`
}

type WorkflowNode struct {
	Node        string                  `json:"node"`
	Children    []string                `json:"children"`
	Skipped     bool                    `json:"skipped"`
	Failed      bool                    `json:"failed"`
	Output      *responses.ModuleResult `json:"output"`
	GlobalLevel int                     `json:"globalLevel"`
	LocalLevel  int                     `json:"localLevel"`
}
