package responses

import "encoding/json"

type WorkflowResultPhase1 struct {
	OutputDataCollection      ModuleResult `json:"outputDataCollection"`
	OutputEvidencesCollection ModuleResult `json:"outputEvidencesCollection"`
}

type WorkflowResultPhase2 struct {
	OutputPep          ModuleResult `json:"outputPep"`
	OutputSanctions    ModuleResult `json:"outputSanctions"`
	OutputAdverseMedia ModuleResult `json:"outputAdverseMedia"`
}

type WorkflowResultPhase3 struct {
	OutputSynthesis ModuleResult `json:"outputSynthesis"`
}

type WorkflowResult struct {
	WorkflowResultPhase1
	WorkflowResultPhase2
	WorkflowResultPhase3
}

func (w WorkflowResult) ToJSON() string {
	result, _ := json.Marshal(w)
	return string(result)
}

type WorkflowUUIDResult struct {
	WorkflowUUID string `json:"workflowUUID"`
}

func (w WorkflowUUIDResult) ToJSON() string {
	result, _ := json.Marshal(w)
	return string(result)
}

type ModuleResult struct {
	ModuleName string `json:"moduleName"`
	Output     string `json:"output,omitempty"`
	Status     string `json:"status"`
}
