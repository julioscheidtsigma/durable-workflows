package steps

import (
	"context"
	"time"

	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/responses"
	"github.com/julioscheidtsigma/dbos/utils"
)

const (
	// steps
	DataCollectionStepName      = "DataCollectionStep"
	EvidencesCollectionStepName = "EvidencesCollectionStep"
	PepStepName                 = "PepStep"
	SanctionsStepName           = "SanctionsStep"
	// statuses
	SkippedStep  = "SKIPPED"
	ExecutedStep = "EXECUTED"
	FailedStep   = "FAILED"
	// params
	Phase1Params = "paramsPhase1"
	Phase2Params = "paramsPhase2"
)

func ParseRunStep(stepStr string) constants.Step {
	step := constants.RUN_STEP_ALL // default to run all steps
	switch stepStr {
	case "0":
		step = constants.RUN_STEP_ALL
	case "1":
		step = constants.RUN_STEP_DATA_COLLECTION
	case "2":
		step = constants.RUN_STEP_EVIDENCES_COLLECTION
	case "3":
		step = constants.RUN_STEP_PEP
	case "4":
		step = constants.RUN_STEP_SANCTIONS
	default:
	}
	return step
}

func PlaceholderStep(ctx context.Context) (any, error) {
	return nil, nil
}

func GenericWorkflowStep(ctx context.Context, output responses.StepResult) (responses.StepResult, error) {
	// inject failure to test retries
	if err := utils.RandomlyFail(); err != nil {
		newOutput := output
		newOutput.Status = FailedStep
		newOutput.Output = err.Error()
		return newOutput, err
	}
	time.Sleep(10 * time.Second)
	return output, nil
}

// phase 1
func DataCollectionStep(ctx context.Context) (responses.StepResult, error) {
	params := ctx.Value(Phase1Params).(requests.WorkflowParamsPhase1)
	response := responses.StepResult{
		StepName: DataCollectionStepName,
		Level:    params.Level,
		Output:   params.Name,
		Status:   ExecutedStep,
	}
	dataCollectionEnabled := ctx.Value("dataCollectionEnabled").(bool)
	if !dataCollectionEnabled {
		response.Status = SkippedStep
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowStep(ctx, response)
}

func EvidencesCollectionStep(ctx context.Context) (responses.StepResult, error) {
	params := ctx.Value(Phase1Params).(requests.WorkflowParamsPhase1)
	response := responses.StepResult{
		StepName: EvidencesCollectionStepName,
		Level:    params.Level,
		Output:   params.Name,
		Status:   ExecutedStep,
	}
	evidencesCollectionEnabled := ctx.Value("evidencesCollectionEnabled").(bool)
	if !evidencesCollectionEnabled {
		response.Status = SkippedStep
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowStep(ctx, response)
}

// phase 2
func PepStep(ctx context.Context) (responses.StepResult, error) {
	params := ctx.Value(Phase2Params).(requests.WorkflowParamsPhase2)
	resultPhase1DCName := params.Phase1.OutputDataCollection.Output
	resultPhase1ECName := params.Phase1.OutputEvidencesCollection.Output
	response := responses.StepResult{
		StepName: PepStepName,
		Level:    params.Level,
		Output:   params.Name + " - DC: " + resultPhase1DCName + " - EC: " + resultPhase1ECName,
		Status:   ExecutedStep,
	}
	pepEnabled := ctx.Value("pepEnabled").(bool)
	if !pepEnabled {
		response.Status = SkippedStep
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowStep(ctx, response)
}

func SanctionsStep(ctx context.Context) (responses.StepResult, error) {
	params := ctx.Value(Phase2Params).(requests.WorkflowParamsPhase2)
	resultPhase1DCName := params.Phase1.OutputDataCollection.Output
	resultPhase1ECName := params.Phase1.OutputEvidencesCollection.Output
	response := responses.StepResult{
		StepName: SanctionsStepName,
		Level:    params.Level,
		Output:   params.Name + " - DC: " + resultPhase1DCName + " - EC: " + resultPhase1ECName,
		Status:   ExecutedStep,
	}
	sanctionsEnabled := ctx.Value("sanctionsEnabled").(bool)
	if !sanctionsEnabled {
		response.Status = SkippedStep
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowStep(ctx, response)
}
