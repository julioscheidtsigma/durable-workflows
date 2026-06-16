package steps

import (
	"context"
	"fmt"
	"time"

	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/utils"
)

func ParseStepFromQuery(stepStr string) constants.Step {
	step := constants.RUN_STEP_ALL // default to run all steps
	switch stepStr {
	case "0":
		step = constants.RUN_STEP_ALL
	case "1":
		step = constants.RUN_STEP_DATA_COLLECTION
	case "2":
		step = constants.RUN_STEP_EVIDENCES_COLLECTION
	case "3":
		step = constants.RUN_STEP_PEP_MODULE
	case "4":
		step = constants.RUN_STEP_SANCTIONS_MODULE
	default:
	}
	return step
}

func GenericWorkflowStep(ctx context.Context, stepName string, fn func()) (string, error) {
	// inject failure to test retries
	if err := utils.RandomlyFail(); err != nil {
		return "", err
	}
	fn()
	return fmt.Sprintf("%s succeeded", stepName), nil
}

func sleep() {
	time.Sleep((10 * time.Second))
}

// phase 1
func DataCollectionStep(ctx context.Context) (string, error) {
	params := ctx.Value("params").(requests.WorkflowParamsPhase1)
	fmt.Printf("DataCollectionStep params: %+v\n", params)
	return GenericWorkflowStep(ctx, "DataCollectionStep", sleep)
}

func EvidencesCollectionStep(ctx context.Context) (string, error) {
	params := ctx.Value("params").(requests.WorkflowParamsPhase1)
	fmt.Printf("EvidencesCollectionStep params: %+v\n", params)
	return GenericWorkflowStep(ctx, "EvidencesCollectionStep", sleep)
}

// phase 2
func PepModuleStep(ctx context.Context) (string, error) {
	params := ctx.Value("params").(requests.WorkflowParamsPhase2)
	fmt.Printf("PepModuleStep params: %+v\n", params)
	return GenericWorkflowStep(ctx, "PepModuleStep", sleep)
}

func SanctionsModuleStep(ctx context.Context) (string, error) {
	params := ctx.Value("params").(requests.WorkflowParamsPhase2)
	fmt.Printf("SanctionsModuleStep params: %+v\n", params)
	return GenericWorkflowStep(ctx, "SanctionsModuleStep", sleep)
}
