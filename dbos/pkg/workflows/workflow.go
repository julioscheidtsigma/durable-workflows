package workflows

import (
	"fmt"

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
	StartLevel     = 1
)

var QueueResultsChan = make(chan responses.WorkflowResult, 100) // buffered channel to hold workflow results when run as queue

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

	// start at level 1
	currentLevel := StartLevel

	// running placehold modules between phases to help in the workflow execution graph
	_, errPhase1 := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(currentLevel, StartLevelName)))
	if errPhase1 != nil {
		return responses.WorkflowResult{}, errPhase1
	}

	currentLevel++ // 2
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

	currentLevel++ // 3
	_, errPhase2 := dbos.RunAsStep(dbosCtx, modules.PlaceholderModule,
		dbos.WithStepName(buildModuleName(currentLevel, StartLevelName)))
	if errPhase2 != nil {
		return responses.WorkflowResult{}, errPhase2
	}

	currentLevel++ // 4
	paramsPhase2 := requests.WorkflowParamsPhase2{
		Level:      currentLevel,
		Name:       params.Name,
		RunModules: params.RunModules,
		Phase1:     resultPhase1, // injecting results from a previous phase
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
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.Phase1Params, params)
	results := &responses.WorkflowResultPhase1{}

	opts1 := utils.GetModuleOpts()
	opts1 = append(opts1, dbos.WithStepName(buildModuleName(params.Level, modules.DataCollectionModuleName)))
	output1, err := dbos.RunAsStep(dbosCtx, modules.DataCollectionModule, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: DataCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputDataCollection = output1

	opts2 := utils.GetModuleOpts()
	opts2 = append(opts2, dbos.WithStepName(buildModuleName(params.Level, modules.EvidencesCollectionModuleName)))
	output2, err := dbos.RunAsStep(dbosCtx, modules.EvidencesCollectionModule, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflowPhase1: EvidencesCollectionStep: error %+v\n", err)
		return responses.WorkflowResultPhase1{}, err
	}
	results.OutputEvidencesCollection = output2

	fmt.Printf("MainWorkflowPhase1: results %+v\n", results)
	return *results, nil
}

func MainWorkflowPhase2(dbosCtx dbos.DBOSContext, params requests.WorkflowParamsPhase2) (responses.WorkflowResultPhase2, error) {
	// inject params into the context so that modules can access it
	dbosCtx = dbosCtx.WithValue(modules.Phase2Params, params)
	results := &responses.WorkflowResultPhase2{}

	opts1 := utils.GetModuleOpts()
	opts1 = append(opts1, dbos.WithStepName(buildModuleName(params.Level, modules.PepModuleName)))
	output1, err := dbos.RunAsStep(dbosCtx, modules.PepModule, opts1...)
	if err != nil {
		fmt.Printf("MainWorkflow: PepStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputPep = output1

	opts2 := utils.GetModuleOpts()
	opts2 = append(opts2, dbos.WithStepName(buildModuleName(params.Level, modules.SanctionsModuleName)))
	output2, err := dbos.RunAsStep(dbosCtx, modules.SanctionsModule, opts2...)
	if err != nil {
		fmt.Printf("MainWorkflow: SanctionsStep: error %+v\n", err)
		return responses.WorkflowResultPhase2{}, err
	}
	results.OutputSanctions = output2

	fmt.Printf("MainWorkflowPhase2: results %+v\n", results)
	return *results, nil
}
