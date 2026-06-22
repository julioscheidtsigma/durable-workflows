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
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

type ModuleResultWithError struct {
	responses.ModuleResult
	err error
}

func buildModuleName(globalLevel int, moduleName string) string {
	return fmt.Sprintf("%s:%d:%s", LevelPrefix, globalLevel, moduleName)
}

func MainWorkflow(dbosCtx dbos.DBOSContext, params requests.WorkflowRequestParams) (responses.WorkflowResult, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue("params", params)
	fmt.Printf("MainWorkflow: params %+v\n", params)

	runAll := params.RunModules.RunAllModules()
	dbosCtx = dbosCtx.WithValue("dataCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_DATA_COLLECTION)
	dbosCtx = dbosCtx.WithValue("evidencesCollectionEnabled", runAll || params.RunModules == constants.RUN_MODULES_EVIDENCES_COLLECTION)
	dbosCtx = dbosCtx.WithValue("pepEnabled", runAll || params.RunModules == constants.RUN_MODULES_PEP)
	dbosCtx = dbosCtx.WithValue("sanctionsEnabled", runAll || params.RunModules == constants.RUN_MODULES_SANCTIONS)
	dbosCtx = dbosCtx.WithValue("synthesisEnabled", runAll || params.RunModules == constants.RUN_MODULES_SYNTHESIS)

	// start at level -1, step -1, to be increased
	wsGlobalParams := requests.WorkflowParamsGlobal{
		Name:          params.Name,
		RunModules:    params.RunModules,
		WorkflowState: requests.NewWorkflowState(),
	}

	// running placehold modules between phases to help in the workflow execution graph
	errPlaceholder1 := MainWorkflowPlaceholder(dbosCtx, &wsGlobalParams)
	if errPlaceholder1 != nil {
		return responses.WorkflowResult{}, errPlaceholder1
	}
	paramsPhase1 := requests.WorkflowParamsPhase1{
		WorkflowParamsGlobal: wsGlobalParams,
	}
	resultPhase1, err := MainWorkflowPhase1(dbosCtx, &paramsPhase1)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	errPlaceholder2 := MainWorkflowPlaceholder(dbosCtx, &wsGlobalParams)
	if errPlaceholder2 != nil {
		return responses.WorkflowResult{}, errPlaceholder2
	}
	paramsPhase2 := requests.WorkflowParamsPhase2{
		WorkflowParamsGlobal: wsGlobalParams,
		Phase1:               resultPhase1, // injecting results from previous phases
	}
	resultPhase2, err := MainWorkflowPhase2(dbosCtx, &paramsPhase2)
	if err != nil {
		return responses.WorkflowResult{}, err
	}

	errPlaceholder3 := MainWorkflowPlaceholder(dbosCtx, &wsGlobalParams)
	if errPlaceholder3 != nil {
		return responses.WorkflowResult{}, errPlaceholder3
	}
	paramsPhase3 := requests.WorkflowParamsPhase3{
		WorkflowParamsGlobal: wsGlobalParams,
		Phase1:               resultPhase1, // injecting results from previous phases
		Phase2:               resultPhase2,
	}
	resultPhase3, err := MainWorkflowPhase3(dbosCtx, &paramsPhase3)
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

func MainWorkflowPlaceholder(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsGlobal) error {
	_, err := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(params.NextGlobalLevel(), StartLevelName)),
		dbos.WithNextStepID(params.NextStepID()),
	)
	return err
}

func MainWorkflowPhase1(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase1) (responses.WorkflowResultPhase1, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase1, *params)
	results := &responses.WorkflowResultPhase1{}

	params.NextGlobalLevel() // increase one global level
	defaultOpts := utils.BuildModuleOpts()

	step1Name := buildModuleName(params.CurrentGlobalLevel(), modules.DataCollectionModuleName)
	opts1 := append(defaultOpts, dbos.WithStepName(step1Name))
	opts1 = append(opts1, dbos.WithNextStepID(params.NextStepID()))

	step2Name := buildModuleName(params.CurrentGlobalLevel(), modules.EvidencesCollectionModuleName)
	opts2 := append(defaultOpts, dbos.WithStepName(step2Name))
	opts2 = append(opts2, dbos.WithNextStepID(params.NextStepID()))

	numModules := 2
	wg := &sync.WaitGroup{}
	wg.Add(numModules)
	resultsChan := make(chan ModuleResultWithError, numModules) // buffered channel to hold results from modules

	go func() {
		defer wg.Done()
		output, err := dbos.RunAsStep(dbosCtx, modules.DataCollectionModule, opts1...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}()

	go func() {
		defer wg.Done()
		output, err := dbos.RunAsStep(dbosCtx, modules.EvidencesCollectionModule, opts2...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}()

	wg.Wait()
	close(resultsChan)

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

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase2, *params)
	results := &responses.WorkflowResultPhase2{}

	params.NextGlobalLevel() // increase one global level
	defaultOpts := utils.BuildModuleOpts()

	step1Name := buildModuleName(params.CurrentGlobalLevel(), modules.PepModuleName)
	opts1 := append(defaultOpts, dbos.WithStepName(step1Name))
	opts1 = append(opts1, dbos.WithNextStepID(params.NextStepID()))

	step2Name := buildModuleName(params.CurrentGlobalLevel(), modules.SanctionsModuleName)
	opts2 := append(defaultOpts, dbos.WithStepName(step2Name))
	opts2 = append(opts2, dbos.WithNextStepID(params.NextStepID()))

	numModules := 2
	wg := &sync.WaitGroup{}
	wg.Add(numModules)
	resultsChan := make(chan ModuleResultWithError, numModules) // buffered channel to hold results from modules

	go func() {
		defer wg.Done()
		output, err := dbos.RunAsStep(dbosCtx, modules.PepModule, opts1...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}()

	go func() {
		defer wg.Done()
		output, err := dbos.RunAsStep(dbosCtx, modules.SanctionsModule, opts2...)
		resultsChan <- ModuleResultWithError{ModuleResult: output, err: err}
	}()

	wg.Wait()
	close(resultsChan)

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

func MainWorkflowPhase3(dbosCtx dbos.DBOSContext, params *requests.WorkflowParamsPhase3) (responses.WorkflowResultPhase3, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.ParamsPhase3, *params)
	results := &responses.WorkflowResultPhase3{}

	params.NextGlobalLevel() // increase one global level
	defaultOpts := utils.BuildModuleOpts()

	step1Name := buildModuleName(params.CurrentGlobalLevel(), modules.SynthesisModuleName)
	opts1 := append(defaultOpts, dbos.WithStepName(step1Name))
	opts1 = append(opts1, dbos.WithNextStepID(params.NextStepID()))

	output, err := dbos.RunAsStep(dbosCtx, modules.SynthesisModule, opts1...)
	if err != nil {
		return responses.WorkflowResultPhase3{}, err
	}
	results.OutputSynthesis = output

	fmt.Printf("MainWorkflowPhase3: results %+v\n", results)
	return *results, nil
}
