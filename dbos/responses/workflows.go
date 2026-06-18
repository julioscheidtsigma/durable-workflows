package responses

import "encoding/json"

type WorkflowResultPhase1 struct {
	OutputDataCollection      StepResult `json:"outputDataCollection"`
	OutputEvidencesCollection StepResult `json:"outputEvidencesCollection"`
}

type WorkflowResultPhase2 struct {
	OutputPep       StepResult `json:"outputPep"`
	OutputSanctions StepResult `json:"outputSanctions"`
}

type WorkflowResult struct {
	Phase1 WorkflowResultPhase1
	Phase2 WorkflowResultPhase2
}

func (wr WorkflowResult) ToJSON() string {
	result, _ := json.Marshal(wr)
	return string(result)
}

type WorkflowUUIDResult struct {
	UUID string `json:"uuid"`
}

func (wu WorkflowUUIDResult) ToJSON() string {
	result, _ := json.Marshal(wu)
	return string(result)
}

type StepResult struct {
	StepName string `json:"stepName"`
	Level    int    `json:"level,omitempty"`
	Output   string `json:"output,omitempty"`
	Status   string `json:"status"`
}
