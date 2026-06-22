package requests

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
)

type WorkflowRequest struct {
	Name       *string `json:"name"`
	RunModules *int    `json:"runModules"` // optional param to control which module to run, default is 0 which means run all modules
}

func (p *WorkflowRequest) Validate() error {
	if p.Name != nil && *p.Name == "" {
		return errors.New("name is required")
	}
	if p.RunModules != nil && (*p.RunModules < 0 || *p.RunModules > 5) {
		return errors.New("runModules must be between 0 and 4")
	}
	return nil
}

// {"positionalArgs":[{"name":"string","runModules":int}],"namedArgs":{}}
type WorkflowParamsWrapper struct {
	PositionalArgs []WorkflowRequestParams `json:"positionalArgs"`
	NamedArgs      map[string]any          `json:"namedArgs"`
}

func NewWorkflowParamsWrapper(name string, runModules constants.Module) WorkflowParamsWrapper {
	return WorkflowParamsWrapper{
		PositionalArgs: []WorkflowRequestParams{
			{Name: name, RunModules: runModules},
		},
		NamedArgs: map[string]any{},
	}
}

func (p WorkflowParamsWrapper) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

type WorkflowRequestParams struct {
	Name       string           `json:"name"`
	RunModules constants.Module `json:"runModules"` // optional param to control which module to run, default is 0 which means run all modules
}

func (p WorkflowRequestParams) ToJSON() string {
	result, _ := json.Marshal(p)
	return string(result)
}

func (p WorkflowRequestParams) IdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.Name)
	_, _ = hash.WriteString(strconv.Itoa(int(p.RunModules)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

type WorkflowState struct {
	stepID        int
	globalLevelID int
}

func (ws *WorkflowState) NextStepID() int {
	ws.stepID++
	return ws.stepID
}

func (ws *WorkflowState) NextGlobalLevel() int {
	ws.globalLevelID++
	return ws.globalLevelID
}

func (ws *WorkflowState) CurrentGlobalLevel() int {
	return ws.globalLevelID
}

func NewWorkflowState() *WorkflowState {
	return &WorkflowState{
		stepID:        -1,
		globalLevelID: -1,
	}
}

type WorkflowGlobalParams struct {
	Name           string           `json:"name"`
	RunModules     constants.Module `json:"runModules"`
	*WorkflowState `json:"-"`
}

type WorkflowParamsPhase1 struct {
	WorkflowGlobalParams
}

type WorkflowParamsPhase2 struct {
	WorkflowGlobalParams
	// this will receive the outputs from phase 1
	Phase1 responses.WorkflowResultPhase1 `json:"outputPhase1"`
}

type WorkflowParamsPhase3 struct {
	WorkflowGlobalParams
	// this will receive the outputs from phase 1 and phase 2
	Phase1 responses.WorkflowResultPhase1 `json:"outputPhase1"`
	Phase2 responses.WorkflowResultPhase2 `json:"outputPhase2"`
}
