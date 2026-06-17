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
	QueueName = "edd-queue"
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowParams) (responses.WorkflowResult, error) {
	// workflow id is the same as the idempotency key
	workflowID, _ := dbosCtx.GetWorkflowID()
	fmt.Printf("MainWorkflow: workflowID %+v\n", workflowID)

	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("params", params)
	fmt.Printf("MainWorkflow: params %+v\n", params)

	// running placehold steps between phases to help in the workflow execution graph
	_, errPhase1 := dbos.RunAsStep(dbosCtx, steps.PlaceholderStep, dbos.WithStepName("StartPhase-1"))
	if errPhase1 != nil {
		return responses.WorkflowResult{}, errPhase1
	}

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

	_, errPhase2 := dbos.RunAsStep(dbosCtx, steps.PlaceholderStep, dbos.WithStepName("StartPhase-2"))
	if errPhase2 != nil {
		return responses.WorkflowResult{}, errPhase2
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
	fmt.Printf("MainWorkflowChildren: params %+v\n", params)

	opts := []dbos.WorkflowOption{}
	opts = append(opts, dbos.WithQueue(QueueName))
	opts = append(opts, dbos.WithPortableWorkflow()) // marks the workflow to use JSON format for all serialized data

	paramsPhase1 := requests.WorkflowParamsPhase1{
		Name: params.Name,
		Step: params.Step,
	}
	workflowPhase1ID := fmt.Sprintf("%s-%d", workflowID, 0)
	optsPhase1 := opts
	optsPhase1 = append(optsPhase1, dbos.WithWorkflowID(workflowPhase1ID))
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	handlePhase1, err := dbos.RunWorkflow(dbosCtx, MainWorkflowPhase1, paramsPhase1, optsPhase1...)
	if err != nil {
		return responses.WorkflowResult{}, err
	}
	resultPhase1, err := handlePhase1.GetResult()
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	paramsPhase2 := requests.WorkflowParamsPhase2{
		Name:         params.Name,
		Step:         params.Step,
		ResultPhase1: resultPhase1,
	}
	workflowPhase2ID := fmt.Sprintf("%s-%d", workflowID, 2)
	optsPhase2 := opts
	optsPhase2 = append(optsPhase2, dbos.WithWorkflowID(workflowPhase2ID))
	handlePhase2, err := dbos.RunWorkflow(dbosCtx, MainWorkflowPhase2, paramsPhase2, optsPhase2...)
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

func MainWorkflowPhase1(dbosCtx dbos.DBOSContext, paramsPhase1 requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("paramsPhase1", paramsPhase1)

	fmt.Printf("MainWorkflowPhase1 paramsPhase1: %+v\n", paramsPhase1)
	opts := utils.GetStepOpts()

	runAllSteps := paramsPhase1.Step.RunAllSteps()
	dbosCtx = dbosCtx.WithValue("dataCollectionEnabled", runAllSteps || paramsPhase1.Step == constants.RUN_STEP_DATA_COLLECTION)
	dbosCtx = dbosCtx.WithValue("evidencesCollectionEnabled", runAllSteps || paramsPhase1.Step == constants.RUN_STEP_EVIDENCES_COLLECTION)
	results := &responses.WorkflowResultPhase1{}

	opts1 := opts
	opts1 = append(opts1, dbos.WithStepName("DataCollectionStep"))
	output1, err := dbos.RunAsStep(dbosCtx, steps.DataCollectionStep, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: DataCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputDataCollection = output1

	opts2 := opts
	opts2 = append(opts2, dbos.WithStepName("EvidencesCollectionStep"))
	output2, err := dbos.RunAsStep(dbosCtx, steps.EvidencesCollectionStep, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: EvidencesCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputEvidencesCollection = output2

	fmt.Printf("MainWorkflowPhase1: results %+v\n", results)

	return *results, nil
}

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, paramsPhase2 requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that steps can access it
	dbosCtx = dbosCtx.WithValue("paramsPhase2", paramsPhase2)

	fmt.Printf("MainWorkflowPhase2 paramsPhase2: %+v\n", paramsPhase2)
	opts := utils.GetStepOpts()

	runAllSteps := paramsPhase2.Step.RunAllSteps()
	dbosCtx = dbosCtx.WithValue("pepModuleEnabled", runAllSteps || paramsPhase2.Step == constants.RUN_STEP_PEP_MODULE)
	dbosCtx = dbosCtx.WithValue("sanctionsModuleEnabled", runAllSteps || paramsPhase2.Step == constants.RUN_STEP_SANCTIONS_MODULE)
	results := &responses.WorkflowResultPhase2{}

	opts1 := opts
	opts1 = append(opts1, dbos.WithStepName("PepModuleStep"))
	output1, err := dbos.RunAsStep(dbosCtx, steps.PepModuleStep, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflow: PepModuleStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputPepModule = output1

	opts2 := opts
	opts2 = append(opts2, dbos.WithStepName("SanctionsModuleStep"))
	output2, err := dbos.RunAsStep(dbosCtx, steps.SanctionsModuleStep, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflow: SanctionsModuleStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputSanctionsModule = output2

	fmt.Printf("MainWorkflowPhase2: results %+v\n", results)

	return *results, nil
}
