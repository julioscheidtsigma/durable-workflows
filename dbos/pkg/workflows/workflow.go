package workflows

import (
	"fmt"
	"slices"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/julioscheidtsigma/dbos/api/requests"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/pkg/constants"
	"github.com/julioscheidtsigma/dbos/pkg/modules"
	"github.com/julioscheidtsigma/dbos/pkg/utils"
)

const (
	LevelPrefix    = "Level"
	StartLevelName = "Start"
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

type ModuleResultWithError struct {
	responses.ModuleResult
	err error
}

func buildModuleName(globalLevel int, moduleName string) string {
	return fmt.Sprintf("%s:%d:%s", LevelPrefix, globalLevel, moduleName)
}

func buildStepOptsFromParams(params *requests.WorkflowGlobalParams, moduleName string) []dbos.StepOption {
	defaultOpts := utils.BuildModuleOpts()
	stepName := buildModuleName(params.CurrentGlobalLevel(), moduleName)

	opts := slices.Clone(defaultOpts)
	opts = append(opts, dbos.WithStepName(stepName))

	return opts
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowRequestParams) (responses.WorkflowResult, error) {
	fmt.Printf("Starting MainWorkflow\n")

	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue("params", params)
	fmt.Printf("MainWorkflow: params %+v\n", params)

	runAll := params.RunModules.RunAllModules()
	// inject params into the dbosCtx to be accessed by modules
	dbosCtx = dbosCtx.WithValue("dataCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_DATA_COLLECTION)
	dbosCtx = dbosCtx.WithValue("evidencesCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_EVIDENCES_COLLECTION)
	dbosCtx = dbosCtx.WithValue("pepEnabled", runAll || params.RunModules == constants.RUN_MODULES_PEP)
	dbosCtx = dbosCtx.WithValue("sanctionsEnabled", runAll || params.RunModules == constants.RUN_MODULES_SANCTIONS)
	dbosCtx = dbosCtx.WithValue("adverseMediaEnabled", runAll || params.RunModules == constants.RUN_MODULES_ADVERSE_MEDIA)
	dbosCtx = dbosCtx.WithValue("synthesisEnabled", runAll || params.RunModules == constants.RUN_MODULES_SYNTHESIS)

	wsGlobalParams := requests.WorkflowGlobalParams{
		Name:          params.Name,
		RunModules:    params.RunModules,
		WorkflowState: requests.NewWorkflowState(),
	}
	fmt.Printf("MainWorkflow: wsGlobalParams %+v\n", wsGlobalParams)

	// running placehold modules between phases to help in the workflow execution graph
	errPlaceholder1 := MainWorkflowPlaceholderWrapper(dbosCtx, &wsGlobalParams)
	if errPlaceholder1 != nil {
		return responses.WorkflowResult{}, errPlaceholder1
	}
	paramsPhase1 := requests.WorkflowParamsPhase1{
		WorkflowGlobalParams: wsGlobalParams,
	}
	resultPhase1, err := MainWorkflowPhase1Wrapper(dbosCtx, &paramsPhase1)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	errPlaceholder2 := MainWorkflowPlaceholderWrapper(dbosCtx, &wsGlobalParams)
	if errPlaceholder2 != nil {
		return responses.WorkflowResult{}, errPlaceholder2
	}
	paramsPhase2 := requests.WorkflowParamsPhase2{
		WorkflowGlobalParams: wsGlobalParams,
		Phase1:               resultPhase1, // injecting results from previous phases
	}
	resultPhase2, err := MainWorkflowPhase2Wrapper(dbosCtx, &paramsPhase2)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	errPlaceholder3 := MainWorkflowPlaceholderWrapper(dbosCtx, &wsGlobalParams)
	if errPlaceholder3 != nil {
		return responses.WorkflowResult{}, errPlaceholder3
	}
	paramsPhase3 := requests.WorkflowParamsPhase3{
		WorkflowGlobalParams: wsGlobalParams,
		Phase1:               resultPhase1, // injecting results from previous phases
		Phase2:               resultPhase2,
	}
	resultPhase3, err := MainWorkflowPhase3Wrapper(dbosCtx, &paramsPhase3)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	results := responses.WorkflowResult{
		WorkflowResultPhase1: resultPhase1,
		WorkflowResultPhase2: resultPhase2,
		WorkflowResultPhase3: resultPhase3,
	}

	// send results to a channel to be consumed by another goroutine
	QueueResultsChan <- results

	return results, nil
}

func MainWorkflowPlaceholderWrapper(dbosCtx dbos.DBOSContext, params *requests.WorkflowGlobalParams) error {
	_, err := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(params.NextGlobalLevel(), StartLevelName)),
	)
	return err
}

func MainWorkflowPhase1Wrapper(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase1, *params)
	results := &responses.WorkflowResultPhase1{}

	params.NextGlobalLevel() // increase one global level
	var outputsChan []<-chan dbos.StepOutcome[responses.ModuleResult]

	// first step
	opts1 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.DataCollectionModuleName)
	ch1, err := dbos.Go(dbosCtx, modules.DataCollectionModule, opts1...)
	if err != nil {
		return responses.WorkflowResultPhase1{}, err
	}
	outputsChan = append(outputsChan, ch1)

	// second step
	opts2 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.EvidencesCollectionModuleName)
	ch2, err := dbos.Go(dbosCtx, modules.EvidencesCollectionModule, opts2...)
	if err != nil {
		return responses.WorkflowResultPhase1{}, err
	}
	outputsChan = append(outputsChan, ch2)

	// collect results
	for _, ch := range outputsChan {
		outcome := <-ch
		if outcome.Err != nil {
			return responses.WorkflowResultPhase1{}, outcome.Err
		}
		switch outcome.Result.ModuleName {
		case modules.DataCollectionModuleName:
			results.OutputDataCollection = outcome.Result
		case modules.EvidencesCollectionModuleName:
			results.OutputEvidencesCollection = outcome.Result
		}
	}

	fmt.Printf("MainWorkflowPhase1Wrapper: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase2Wrapper(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase2, *params)
	results := &responses.WorkflowResultPhase2{}

	params.NextGlobalLevel() // increase one global level
	var outputsChan []<-chan dbos.StepOutcome[responses.ModuleResult]

	// first step
	opts1 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.PepModuleName)
	ch1, err := dbos.Go(dbosCtx, modules.PepModule, opts1...)
	if err != nil {
		return responses.WorkflowResultPhase2{}, err
	}
	outputsChan = append(outputsChan, ch1)

	// second step
	opts2 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.SanctionsModuleName)
	ch2, err := dbos.Go(dbosCtx, modules.SanctionsModule, opts2...)
	if err != nil {
		return responses.WorkflowResultPhase2{}, err
	}
	outputsChan = append(outputsChan, ch2)

	// third step
	opts3 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.AdverseMediaModuleName)
	ch3, err := dbos.Go(dbosCtx, modules.AdverseMediaModule, opts3...)
	if err != nil {
		return responses.WorkflowResultPhase2{}, err
	}
	outputsChan = append(outputsChan, ch3)

	// collect results
	for _, ch := range outputsChan {
		outcome := <-ch
		if outcome.Err != nil {
			return responses.WorkflowResultPhase2{}, outcome.Err
		}
		switch outcome.Result.ModuleName {
		case modules.PepModuleName:
			results.OutputPep = outcome.Result
		case modules.SanctionsModuleName:
			results.OutputSanctions = outcome.Result
		case modules.AdverseMediaModuleName:
			results.OutputAdverseMedia = outcome.Result
		}
	}

	fmt.Printf("MainWorkflowPhase2Wrapper: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase3Wrapper(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase3) (responses.WorkflowResultPhase3, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase3, *params)
	results := &responses.WorkflowResultPhase3{}

	params.NextGlobalLevel() // increase one global level

	// first step
	opts1 := buildStepOptsFromParams(&params.WorkflowGlobalParams, modules.SynthesisModuleName)
	output1, err := dbos.RunAsStep(dbosCtx, modules.SynthesisModule, opts1...)
	if err != nil {
		return responses.WorkflowResultPhase3{}, err
	}
	results.OutputSynthesis = output1

	fmt.Printf("MainWorkflowPhase3Wrapper: results %+v\n", results)
	return *results, nil
}
