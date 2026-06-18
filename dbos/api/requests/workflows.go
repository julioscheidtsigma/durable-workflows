package requests

import (
	"encoding/json"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
)

// {"positionalArgs":[{"name":"string","runModules":int}],"namedArgs":{}}
type WorkflowParamsWrapper struct {
	PositionalArgs []WorkflowParams `json:"positionalArgs"`
	NamedArgs      map[string]any   `json:"namedArgs"`
}

func NewWorkflowParamsWrapper(name string, runModules constants.Module) WorkflowParamsWrapper {
	return WorkflowParamsWrapper{
		PositionalArgs: []WorkflowParams{
			{Name: name, RunModules: runModules},
		},
		NamedArgs: map[string]any{},
	}
}

func (p WorkflowParamsWrapper) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

type WorkflowParams struct {
	Name       string           `json:"name"`
	RunModules constants.Module `json:"runModules"` // optional param to control which module to run, default is 0 which means run all modules
}

func (p WorkflowParams) IdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.Name)
	_, _ = hash.WriteString(strconv.Itoa(int(p.RunModules)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

func (p WorkflowParams) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

type WorkflowParamsPhase1 struct {
	Level      int              `json:"level"`
	Name       string           `json:"name"`
	RunModules constants.Module `json:"runModules"`
}

type WorkflowParamsPhase2 struct {
	Level      int              `json:"level"`
	Name       string           `json:"name"`
	RunModules constants.Module `json:"runModules"`
	// this will receive the outputs from phase 1
	Phase1 responses.WorkflowResultPhase1 `json:"outputPhase1"`
}
