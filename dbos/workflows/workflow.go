package workflows

import (
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/julioscheidtsigma/dbos/constants"
	"github.com/julioscheidtsigma/dbos/requests"
	"github.com/julioscheidtsigma/dbos/responses"
	"github.com/julioscheidtsigma/dbos/steps"
	"github.com/julioscheidtsigma/dbos/utils"
)

const (
	QueueName       = "edd-queue"
	StepLevelPrefix = "Level"
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

func buildStepName(level int, stepName string) string {
	return fmt.Sprintf("%s:%d:%s", StepLevelPrefix, level, stepName)
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowParams) (responses.WorkflowResult, error) {
	// workflowID, _ := dbosCtx.GetWorkflowID()

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)
	fmt.Printf("MainWorkflow: params %+v\n", params)

	runAllSteps := params.RunStep.RunAllSteps()
	dbosCtx = dbosCtx.WithValue("dataCollectionEnabled", runAllSteps || params.RunStep == constants.RUN_STEP_DATA_COLLECTION)
	dbosCtx = dbosCtx.WithValue("evidencesCollectionEnabled", runAllSteps || params.RunStep == constants.RUN_STEP_EVIDENCES_COLLECTION)
	dbosCtx = dbosCtx.WithValue("pepEnabled", runAllSteps || params.RunStep == constants.RUN_STEP_PEP)
	dbosCtx = dbosCtx.WithValue("sanctionsEnabled", runAllSteps || params.RunStep == constants.RUN_STEP_SANCTIONS)

	// start at level 1
	currentLevel := 1

	// running placehold steps between phases to help in the workflow execution graph
	_, errPhase1 := dbos.RunAsStep(dbosCtx, steps.PlaceholderStep,
		dbos.WithStepName(buildStepName(currentLevel, "Start")))
	if errPhase1 != nil {
		return responses.WorkflowResult{}, errPhase1
	}

	currentLevel++ // 2
	paramsPhase1 := requests.WorkflowParamsPhase1{
		Level:   currentLevel,
		Name:    params.Name,
		RunStep: params.RunStep,
	}
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	resultPhase1, err := MainWorkflowPhase1(dbosCtx, paramsPhase1)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	currentLevel++ // 3
	_, errPhase2 := dbos.RunAsStep(dbosCtx, steps.PlaceholderStep,
		dbos.WithStepName(buildStepName(currentLevel, "Start")))
	if errPhase2 != nil {
		return responses.WorkflowResult{}, errPhase2
	}

	currentLevel++ // 4
	paramsPhase2 := requests.WorkflowParamsPhase2{
		Level:   currentLevel,
		Name:    params.Name,
		RunStep: params.RunStep,
		Phase1:  resultPhase1, // injecting results from a previous phase
	}
	resultPhase2, err := MainWorkflowPhase2(dbosCtx, paramsPhase2)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	results := responses.WorkflowResult{
		WorkflowResultPhase1: resultPhase1,
		WorkflowResultPhase2: resultPhase2,
	}

	// send results to a channel to be consumed by another goroutine
	QueueResultsChan <- results

	return results, nil
}

func MainWorkflowPhase1(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue(steps.Phase1Params, params)
	// fmt.Printf("MainWorkflowPhase1 params: %+v\n", params)
	results := &responses.WorkflowResultPhase1{}

	opts1 := utils.GetStepOpts()
	opts1 = append(opts1, dbos.WithStepName(buildStepName(params.Level, steps.DataCollectionStepName)))
	output1, err := dbos.RunAsStep(dbosCtx, steps.DataCollectionStep, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: DataCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputDataCollection = output1

	opts2 := utils.GetStepOpts()
	opts2 = append(opts2, dbos.WithStepName(buildStepName(params.Level, steps.EvidencesCollectionStepName)))
	output2, err := dbos.RunAsStep(dbosCtx, steps.EvidencesCollectionStep, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: EvidencesCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputEvidencesCollection = output2

	// fmt.Printf("MainWorkflowPhase1: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue(steps.Phase2Params, params)
	// fmt.Printf("MainWorkflowPhase2 params: %+v\n", params)
	results := &responses.WorkflowResultPhase2{}

	opts1 := utils.GetStepOpts()
	opts1 = append(opts1, dbos.WithStepName(buildStepName(params.Level, steps.PepStepName)))
	output1, err := dbos.RunAsStep(dbosCtx, steps.PepStep, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflow: PepStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputPep = output1

	opts2 := utils.GetStepOpts()
	opts2 = append(opts2, dbos.WithStepName(buildStepName(params.Level, steps.SanctionsStepName)))
	output2, err := dbos.RunAsStep(dbosCtx, steps.SanctionsStep, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflow: SanctionsStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputSanctions = output2

	// fmt.Printf("MainWorkflowPhase2: results %+v\n", results)
	return *results, nil
}
