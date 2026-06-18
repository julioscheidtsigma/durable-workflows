package requests

import (
	"encoding/json"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/responses"
)

// {"positionalArgs":[{"name":"string","runStep":int}],"namedArgs":{}}
type WorkflowParamsWrapper struct {
	PositionalArgs []WorkflowParams `json:"positionalArgs"`
	NamedArgs      map[string]any   `json:"namedArgs"`
}

func NewWorkflowParamsWrapper(name string, runStep constants.Step) WorkflowParamsWrapper {
	return WorkflowParamsWrapper{
		PositionalArgs: []WorkflowParams{
			{Name: name, RunStep: runStep},
		},
		NamedArgs: map[string]any{},
	}
}

func (p WorkflowParamsWrapper) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

type WorkflowParams struct {
	Name    string         `json:"name"`
	RunStep constants.Step `json:"runStep"` // optional param to control which step to run, default is 0 which means run all steps
}

func (p WorkflowParams) IdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.Name)
	_, _ = hash.WriteString(strconv.Itoa(int(p.RunStep)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

func (p WorkflowParams) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

type WorkflowParamsPhase1 struct {
	Level   int            `json:"level"`
	Name    string         `json:"name"`
	RunStep constants.Step `json:"runStep"`
}

type WorkflowParamsPhase2 struct {
	Level   int            `json:"level"`
	Name    string         `json:"name"`
	RunStep constants.Step `json:"runStep"`
	// this will receive the outputs from phase 1
	Phase1 responses.WorkflowResultPhase1 `json:"outputPhase1"`
}
