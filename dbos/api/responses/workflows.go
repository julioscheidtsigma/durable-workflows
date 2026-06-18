package responses

import "encoding/json"

type WorkflowResultPhase1 struct {
	OutputDataCollection      ModuleResult `json:"outputDataCollection"`
	OutputEvidencesCollection ModuleResult `json:"outputEvidencesCollection"`
}

type WorkflowResultPhase2 struct {
	OutputPep       ModuleResult `json:"outputPep"`
	OutputSanctions ModuleResult `json:"outputSanctions"`
}

type WorkflowResult struct {
	WorkflowResultPhase1
	WorkflowResultPhase2
}

func (w WorkflowResult) ToJSON() string {
	result, _ := json.Marshal(w)
	return string(result)
}

type WorkflowUUIDResult struct {
	UUID string `json:"uuid"`
}

func (w WorkflowUUIDResult) ToJSON() string {
	result, _ := json.Marshal(w)
	return string(result)
}

type ModuleResult struct {
	ModuleName string `json:"moduleName"`
	Level      int    `json:"level,omitempty"`
	Output     string `json:"output,omitempty"`
	Status     string `json:"status"`
}
