package requests

import (
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/responses"
)

type WorkflowParams struct {
	Name string         `json:"name"`
	Step constants.Step `json:"step"` // optional param to control which step to run, default is 0 which means run all steps
}

func (p WorkflowParams) IdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.Name)
	_, _ = hash.WriteString(strconv.Itoa(int(p.Step)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

type WorkflowParamsPhase1 struct {
	Name string         `json:"name"`
	Step constants.Step `json:"step"`
}

type WorkflowParamsPhase2 struct {
	Name         string                         `json:"name"`
	Step         constants.Step                 `json:"step"`
	ResultPhase1 responses.WorkflowResultPhase1 `json:"outputPhase1"`
}
