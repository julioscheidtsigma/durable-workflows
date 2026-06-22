package workflows

import (
	"fmt"
	"sync"

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
	StartLevel     = 0
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

type ModuleResultWithError struct {
	responses.ModuleResult
	err error
}

func buildModuleName(level int, moduleName string) string {
	return fmt.Sprintf("%s:%d:%s", LevelPrefix, level, moduleName)
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowParams) (responses.WorkflowResult, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue("params", params)
	fmt.Printf("MainWorkflow: params %+v\n", params)

	runAll := params.RunModules.RunAllModules()
	dbosCtx = dbosCtx.WithValue("dataCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_DATA_COLLECTION)
	dbosCtx = dbosCtx.WithValue("evidencesCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_EVIDENCES_COLLECTION)
	dbosCtx = dbosCtx.WithValue("pepEnabled", runAll || params.RunModules == constants.RUN_MODULES_PEP)
	dbosCtx = dbosCtx.WithValue("sanctionsEnabled", runAll || params.RunModules == constants.RUN_MODULES_SANCTIONS)
	dbosCtx = dbosCtx.WithValue("synthesisEnabled", runAll || params.RunModules == constants.RUN_MODULES_SYNTHESIS)

	// start at level 0
	currentLevel := StartLevel

	// running placehold modules between phases to help in the workflow execution graph
	_, errPhase1 := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(currentLevel, StartLevelName)))
	if errPhase1 != nil {
		return responses.WorkflowResult{}, errPhase1
	}

	currentLevel++ // 1
	paramsPhase1 := requests.WorkflowParamsPhase1{
		Level:      currentLevel,
		Name:       params.Name,
		RunModules: params.RunModules,
	}
	// here we are calling the results right after starting the phase,
	// to simulate dependencies between phases
	resultPhase1, err := MainWorkflowPhase1(dbosCtx, paramsPhase1)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	currentLevel++ // 2
	_, errPhase2 := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(currentLevel, StartLevelName)))
	if errPhase2 != nil {
		return responses.WorkflowResult{}, errPhase2
	}

	currentLevel++ // 3
	paramsPhase2 := requests.WorkflowParamsPhase2{
		Level:      currentLevel,
		Name:       params.Name,
		RunModules: params.RunModules,
		Phase1:     resultPhase1, // injecting results from previous phases
	}
	resultPhase2, err := MainWorkflowPhase2(dbosCtx, paramsPhase2)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	currentLevel++ // 4
	_, errPhase3 := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(currentLevel, StartLevelName)))
	if errPhase3 != nil {
		return responses.WorkflowResult{}, errPhase3
	}

	currentLevel++ // 5
	paramsPhase3 := requests.WorkflowParamsPhase3{
		Level:      currentLevel,
		Name:       params.Name,
		RunModules: params.RunModules,
		Phase1:     resultPhase1, // injecting results from previous phases
		Phase2:     resultPhase2,
	}
	resultPhase3, err := MainWorkflowPhase3(dbosCtx, paramsPhase3)
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

func MainWorkflowPhase1(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase1, params)
	results := &responses.WorkflowResultPhase1{}

	resultsChan := make(chan ModuleResultWithError, 2) // buffered channel to hold results from modules
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func(params requests.WorkflowParamsPhase1) {
		defer wg.Done()
		opts := utils.BuildModuleOpts()
		opts = append(opts, dbos.WithStepName(buildModuleName(params.Level, modules.DataCollectionModuleName)))
		output, err := dbos.RunAsStep(dbosCtx, modules.DataCollectionModule, opts...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}(params)

	go func(params requests.WorkflowParamsPhase1) {
		defer wg.Done()
		opts := utils.BuildModuleOpts()
		opts = append(opts, dbos.WithStepName(buildModuleName(params.Level, modules.EvidencesCollectionModuleName)))
		output, err := dbos.RunAsStep(dbosCtx, modules.EvidencesCollectionModule, opts...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}(params)

	// wait for all goroutines to finish
	wg.Wait()
	close(resultsChan)

	// collect results from the channel
	for output := range resultsChan {
		if output.err != nil {
			return responses.WorkflowResultPhase1{}, output.err
		}
		switch output.ModuleName {
		case modules.DataCollectionModuleName:
			results.OutputDataCollection = output.ModuleResult
		case modules.EvidencesCollectionModuleName:
			results.OutputEvidencesCollection = output.ModuleResult
		}
	}

	fmt.Printf("MainWorkflowPhase1: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase2, params)
	results := &responses.WorkflowResultPhase2{}

	resultsChan := make(chan ModuleResultWithError, 2) // buffered channel to hold results from modules
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func(params requests.WorkflowParamsPhase2) {
		defer wg.Done()
		opts := utils.BuildModuleOpts()
		opts = append(opts, dbos.WithStepName(buildModuleName(params.Level, modules.PepModuleName)))
		output, err := dbos.RunAsStep(dbosCtx, modules.PepModule, opts...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}(params)

	go func(params requests.WorkflowParamsPhase2) {
		defer wg.Done()
		opts := utils.BuildModuleOpts()
		opts = append(opts, dbos.WithStepName(buildModuleName(params.Level, modules.SanctionsModuleName)))
		output, err := dbos.RunAsStep(dbosCtx, modules.SanctionsModule, opts...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}(params)

	// wait for all goroutines to finish
	wg.Wait()
	close(resultsChan)

	// collect results from the channel
	for output := range resultsChan {
		if output.err != nil {
			return responses.WorkflowResultPhase2{}, output.err
		}
		switch output.ModuleName {
		case modules.PepModuleName:
			results.OutputPep = output.ModuleResult
		case modules.SanctionsModuleName:
			results.OutputSanctions = output.ModuleResult
		}
	}

	fmt.Printf("MainWorkflowPhase2: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase3(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase3) (responses.WorkflowResultPhase3, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase3, params)
	results := &responses.WorkflowResultPhase3{}

	opts := utils.BuildModuleOpts()
	opts = append(opts, dbos.WithStepName(buildModuleName(params.Level, modules.SynthesisModuleName)))
	output, err := dbos.RunAsStep(dbosCtx, modules.SynthesisModule, opts...)
	if err != nil {
		return responses.WorkflowResultPhase3{}, err
	}
	results.OutputSynthesis = output

	fmt.Printf("MainWorkflowPhase3: results %+v\n", results)
	return *results, nil
}
