package modules

import (
	"context"
	"time"

	"github.com/julioscheidtsigma/dbos/api/requests"
	"github.com/julioscheidtsigma/dbos/api/responses"
	"github.com/julioscheidtsigma/dbos/pkg/utils"
)

var (
	executionDelay = 10 * time.Second
)

const (
	// modules
	// phase 1
	DataCollectionModuleName      = "DataCollectionModule"
	EvidencesCollectionModuleName = "EvidencesCollectionModule"
	// phase 2
	PepModuleName          = "PepModule"
	SanctionsModuleName    = "SanctionsModule"
	AdverseMediaModuleName = "AdverseMediaModule"
	// phase 3
	SynthesisModuleName = "SynthesisModule"
	// statuses
	SkippedModule  = "SKIPPED"
	ExecutedModule = "EXECUTED"
	FailedModule   = "FAILED"
	// params
	ParamsPhase1 = "paramsPhase1"
	ParamsPhase2 = "paramsPhase2"
	ParamsPhase3 = "paramsPhase3"
)

func PlaceholderModule(ctx context.Context) (any, error) {
	return nil, nil
}

func GenericWorkflowModule(ctx context.Context, output responses.ModuleResult) (responses.ModuleResult, error) {
	// inject failure to test retries
	if err := utils.RandomlyFail(); err != nil {
		newOutput := output
		newOutput.Status = FailedModule
		newOutput.Output = err.Error()
		return newOutput, err
	}
	time.Sleep(executionDelay)
	return output, nil
}

// phase 1
func DataCollectionModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase1).(requests.WorkflowParamsPhase1)
	response := responses.ModuleResult{
		ModuleName: DataCollectionModuleName,
		Output:     params.Name,
		Status:     ExecutedModule,
	}
	dataCollectionEnabled := ctx.Value("dataCollectionEnabled").(bool)
	if !dataCollectionEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}

func EvidencesCollectionModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase1).(requests.WorkflowParamsPhase1)
	response := responses.ModuleResult{
		ModuleName: EvidencesCollectionModuleName,
		Output:     params.Name,
		Status:     ExecutedModule,
	}
	evidencesCollectionEnabled := ctx.Value("evidencesCollectionEnabled").(bool)
	if !evidencesCollectionEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}

// phase 2
func PepModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase2).(requests.WorkflowParamsPhase2)

	resultPhase1DC := params.Phase1.OutputDataCollection.Output
	resultPhase1EC := params.Phase1.OutputEvidencesCollection.Output

	response := responses.ModuleResult{
		ModuleName: PepModuleName,
		Output:     params.Name + " - DC: " + resultPhase1DC + " - EC: " + resultPhase1EC,
		Status:     ExecutedModule,
	}
	pepEnabled := ctx.Value("pepEnabled").(bool)
	if !pepEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}

func SanctionsModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase2).(requests.WorkflowParamsPhase2)

	resultPhase1DC := params.Phase1.OutputDataCollection.Output
	resultPhase1EC := params.Phase1.OutputEvidencesCollection.Output

	response := responses.ModuleResult{
		ModuleName: SanctionsModuleName,
		Output:     params.Name + " - DC: " + resultPhase1DC + " - EC: " + resultPhase1EC,
		Status:     ExecutedModule,
	}
	sanctionsEnabled := ctx.Value("sanctionsEnabled").(bool)
	if !sanctionsEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}

func AdverseMediaModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase2).(requests.WorkflowParamsPhase2)

	resultPhase1DC := params.Phase1.OutputDataCollection.Output
	resultPhase1EC := params.Phase1.OutputEvidencesCollection.Output

	response := responses.ModuleResult{
		ModuleName: AdverseMediaModuleName,
		Output:     params.Name + " - DC: " + resultPhase1DC + " - EC: " + resultPhase1EC,
		Status:     ExecutedModule,
	}
	adverseMediaEnabled := ctx.Value("adverseMediaEnabled").(bool)
	if !adverseMediaEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}

// phase 3
func SynthesisModule(ctx context.Context) (responses.ModuleResult, error) {
	params := ctx.Value(ParamsPhase3).(requests.WorkflowParamsPhase3)

	resultPhase1DC := params.Phase1.OutputDataCollection.Output
	resultPhase1EC := params.Phase1.OutputEvidencesCollection.Output

	resultPhase2Pep := params.Phase2.OutputPep.Output
	resultPhase2Sanctions := params.Phase2.OutputSanctions.Output
	resultPhase2AdverseMedia := params.Phase2.OutputAdverseMedia.Output

	response := responses.ModuleResult{
		ModuleName: SynthesisModuleName,
		Output: params.Name + " - DC: " + resultPhase1DC + " - EC: " + resultPhase1EC +
			" - Pep: " + resultPhase2Pep + " - Sanctions: " + resultPhase2Sanctions +
			" - AdverseMedia: " + resultPhase2AdverseMedia,
		Status: ExecutedModule,
	}
	synthesisEnabled := ctx.Value("synthesisEnabled").(bool)
	if !synthesisEnabled {
		response.Status = SkippedModule
		response.Output = ""
		return response, nil
	}
	return GenericWorkflowModule(ctx, response)
}
