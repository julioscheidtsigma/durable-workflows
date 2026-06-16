package responses

import "encoding/json"

type WorkflowResultPhase1 struct {
	OutputDataCollection      string `json:"outputDataCollection"`
	OutputEvidencesCollection string `json:"outputEvidencesCollection"`
}

type WorkflowResultPhase2 struct {
	OutputPepModule       string `json:"outputPepModule"`
	OutputSanctionsModule string `json:"outputSanctionsModule"`
}

type WorkflowResult struct {
	WorkflowResultPhase1 // phase 1
	WorkflowResultPhase2 // phase 2
}

func (wr WorkflowResult) ToJSON() string {
	result, _ := json.Marshal(wr)
	return string(result)
}
