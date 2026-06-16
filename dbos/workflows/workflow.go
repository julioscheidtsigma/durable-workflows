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
	QUEUE = "edd-queue"
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowParams) (responses.WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflow: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	fmt.Printf("MainWorkflow params: %+v\n", params)

	// run both steps sequentially
	paramsPhase1 := requests.WorkflowParamsPhase1{
		Name: params.Name,
		Step: params.Step,
	}
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	resultPhase1, err := MainWorkflowPhase1(dbosCtx, paramsPhase1)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	paramsPhase2 := requests.WorkflowParamsPhase2{
		Name:         params.Name,
		Step:         params.Step,
		ResultPhase1: resultPhase1,
	}
	resultPhase2, err := MainWorkflowPhase2(dbosCtx, paramsPhase2)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	results := responses.WorkflowResult{
		WorkflowResultPhase1: resultPhase1,
		WorkflowResultPhase2: resultPhase2,
	}

	fmt.Printf("MainWorkflow: results %+v\n", results)
	// send results to a channel to be consumed by another goroutine
	QueueResultsChan <- results

	return results, nil
}

func MainWorkflowChildren(dbosCtx dbos.DBOSContext, params requests.WorkflowParams) (responses.WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflowChildren: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	paramsPhase1 := requests.WorkflowParamsPhase1{
		Name: params.Name,
		Step: params.Step,
	}
	handlePhase1, err := dbos.RunWorkflow(dbosCtx, MainWorkflowPhase1, paramsPhase1, dbos.WithQueue(QUEUE))
	if err != nil {
		return responses.WorkflowResult{}, err
	}
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	resultPhase1, err := handlePhase1.GetResult()
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	paramsPhase2 := requests.WorkflowParamsPhase2{
		Name:         params.Name,
		Step:         params.Step,
		ResultPhase1: resultPhase1,
	}
	handlePhase2, err := dbos.RunWorkflow(dbosCtx, MainWorkflowPhase2, paramsPhase2, dbos.WithQueue(QUEUE))
	if err != nil {
		return responses.WorkflowResult{}, err
	}
	resultPhase2, err := handlePhase2.GetResult()
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	results := responses.WorkflowResult{
		WorkflowResultPhase1: resultPhase1,
		WorkflowResultPhase2: resultPhase2,
	}

	fmt.Printf("MainWorkflowChildren: results %+v\n", results)
	// send results to a channel to be consumed by another goroutine
	QueueResultsChan <- results

	return results, nil
}

func MainWorkflowPhase1(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflowPhase1: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	fmt.Printf("MainWorkflowPhase1 params: %+v\n", params)
	opts := utils.GetStepOpts()

	runAllSteps := params.Step.RunAllSteps()
	results := &responses.WorkflowResultPhase1{}

	// run both steps in parallel
	if runAllSteps || params.Step == constants.RUN_STEP_DATA_COLLECTION {
		output, err := dbos.RunAsStep(dbosCtx, steps.DataCollectionStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowPhase1: DataCollectionStep: error %+v\n", err)
			return responses.WorkflowResultPhase1{}, err // return early if this step fails, can be changed based on the requirement
		}
		fmt.Printf("MainWorkflowPhase1: DataCollectionStep result: %+v\n", output)
		results.OutputDataCollection = output
	}

	if runAllSteps || params.Step == constants.RUN_STEP_EVIDENCES_COLLECTION {
		output, err := dbos.RunAsStep(dbosCtx, steps.EvidencesCollectionStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflowPhase1: EvidencesCollectionStep: error %+v\n", err)
			return responses.WorkflowResultPhase1{}, err // return early if this step fails, can be changed based on the requirement
		}
		fmt.Printf("MainWorkflowPhase1: EvidencesCollectionStep result: %+v\n", output)
		results.OutputEvidencesCollection = output
	}
	fmt.Printf("MainWorkflowPhase1: results %+v\n", results)

	return *results, nil
}

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflowPhase2: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)

	fmt.Printf("MainWorkflowPhase2 params: %+v\n", params)
	opts := utils.GetStepOpts()

	runAllSteps := params.Step.RunAllSteps()
	results := &responses.WorkflowResultPhase2{}

	// run both steps in parallel
	if runAllSteps || params.Step == constants.RUN_STEP_PEP_MODULE {
		output, err := dbos.RunAsStep(dbosCtx, steps.PepModuleStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflow: PepModuleStep: error %+v\n", err)
			return responses.WorkflowResultPhase2{}, err // return early if this step fails, can be changed based on the requirement
		}
		fmt.Printf("MainWorkflow: PepModuleStep result: %+v\n", output)
		results.OutputPepModule = output
	}

	if runAllSteps || params.Step == constants.RUN_STEP_SANCTIONS_MODULE {
		output, err := dbos.RunAsStep(dbosCtx, steps.SanctionsModuleStep, opts...)
		if err != nil {
			fmt.Printf("MainWorkflow: SanctionsModuleStep: error %+v\n", err)
			return responses.WorkflowResultPhase2{}, err // return early if this step fails, can be changed based on the requirement
		}
		fmt.Printf("MainWorkflow: SanctionsModuleStep result: %+v\n", output)
		results.OutputSanctionsModule = output
	}
	fmt.Printf("MainWorkflowPhase2: results %+v\n", results)

	return *results, nil
}
