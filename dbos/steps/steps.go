package steps

import (
	"context"
	"time"

	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/utils"
)

const (
	SKIPPED_STEP = "SKIPPED"
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

func GenericWorkflowStep(ctx context.Context, output string) (string, error) {
	// inject failure to test retries
	if err := utils.RandomlyFail(); err != nil {
		return "", err
	}
	time.Sleep(10 * time.Second)
	return output, nil
}

func PlaceholderStep(ctx context.Context) (any, error) {
	return nil, nil
}

// phase 1
func DataCollectionStep(ctx context.Context) (string, error) {
	dataCollectionEnabled := ctx.Value("dataCollectionEnabled").(bool)
	if !dataCollectionEnabled {
		return SKIPPED_STEP, nil
	}
	paramsPhase1 := ctx.Value("paramsPhase1").(requests.WorkflowParamsPhase1)
	return GenericWorkflowStep(ctx, "DataCollectionStep - Name: \""+paramsPhase1.Name+"\"")
}

func EvidencesCollectionStep(ctx context.Context) (string, error) {
	evidencesCollectionEnabled := ctx.Value("evidencesCollectionEnabled").(bool)
	if !evidencesCollectionEnabled {
		return SKIPPED_STEP, nil
	}
	paramsPhase1 := ctx.Value("paramsPhase1").(requests.WorkflowParamsPhase1)
	return GenericWorkflowStep(ctx, "EvidencesCollectionStep - Name: \""+paramsPhase1.Name+"\"")
}

// phase 2
func PepModuleStep(ctx context.Context) (string, error) {
	pepModuleEnabled := ctx.Value("pepModuleEnabled").(bool)
	if !pepModuleEnabled {
		return SKIPPED_STEP, nil
	}
	paramsPhase2 := ctx.Value("paramsPhase2").(requests.WorkflowParamsPhase2)
	return GenericWorkflowStep(ctx, "PepModuleStep - Name: \""+paramsPhase2.Name+"\"")
}

func SanctionsModuleStep(ctx context.Context) (string, error) {
	sanctionsModuleEnabled := ctx.Value("sanctionsModuleEnabled").(bool)
	if !sanctionsModuleEnabled {
		return SKIPPED_STEP, nil
	}
	paramsPhase2 := ctx.Value("paramsPhase2").(requests.WorkflowParamsPhase2)
	return GenericWorkflowStep(ctx, "SanctionsModuleStep - Name: \""+paramsPhase2.Name+"\"")
}
