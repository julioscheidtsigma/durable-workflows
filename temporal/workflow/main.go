package workflow

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	RUN_STEP_0 int = iota // run all steps
	RUN_STEP_1            // run only step 1
	RUN_STEP_2            // run only step 2
)

const QUEUE = "edd-queue"

type WorkflowParams struct {
	URN     string `json:"urn"`
	RunStep int    `json:"runStep"` // optional param to control which step to run, default is 0 which means run all steps
}

type WorkflowResult struct {
	OutputStep1 string `json:"outputStep1"`
	OutputStep2 string `json:"outputStep2"`
}

func (p WorkflowParams) GetIdempotencyKey() string {
	hash := xxhash.New()
	_, _ = hash.WriteString(p.URN)
	_, _ = hash.WriteString(strconv.Itoa(int(p.RunStep)))
	return strconv.FormatUint(hash.Sum64(), 10)
}

type Activities struct {
	TaskQueue string
}

func NewActivities(taskQueue string) *Activities {
	return &Activities{
		TaskQueue: taskQueue,
	}
}

func (a *Activities) GenericWorkflowStep(ctx context.Context, stepName string, params WorkflowParams) (string, error) {
	// inject random failure to test retries
	randNum := rand.IntN(2) // generates a random number between 0 and 1
	if randNum == 1 {
		return "", errors.New("simulated error in the step")
	}
	return fmt.Sprintf("%s succeeded", stepName), nil
}

func (a *Activities) FirstWorkflowStep(ctx context.Context, params WorkflowParams) (string, error) {
	return a.GenericWorkflowStep(ctx, "FirstWorkflowStep", params)
}

func (a *Activities) SecondWorkflowStep(ctx context.Context, params WorkflowParams) (string, error) {
	return a.GenericWorkflowStep(ctx, "SecondWorkflowStep", params)
}

func MainWorkflowChild(ctx workflow.Context, params WorkflowParams) (string, error) {
	fmt.Printf("MainWorkflowChild params: %+v\n", params)

	return "MainWorkflowChild succeeded", nil
}

func MainWorkflow(ctx workflow.Context, params WorkflowParams) (WorkflowResult, error) {
	fmt.Printf("MainWorkflow params: %+v\n", params)

	activities := NewActivities(QUEUE)

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    5,
	}

	opts := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 1,
		HeartbeatTimeout:    0,
		TaskQueue:           activities.TaskQueue,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, opts)

	var err error
	var outputStep1 string
	var outputStep2 string
	runAllSteps := params.RunStep == RUN_STEP_0

	if runAllSteps || params.RunStep == RUN_STEP_1 {
		// if err = workflow.ExecuteActivity(ctx, "FirstWorkflowStep", params).
		if err = workflow.
			ExecuteActivity(ctx, activities.FirstWorkflowStep, params).
			Get(ctx, &outputStep1); err != nil {
			return WorkflowResult{}, err
		}
	}

	if runAllSteps || params.RunStep == RUN_STEP_2 {
		// if err = workflow.ExecuteActivity(ctx, "SecondWorkflowStep", params).
		if err = workflow.
			ExecuteActivity(ctx, activities.SecondWorkflowStep, params).
			Get(ctx, &outputStep2); err != nil {
			return WorkflowResult{}, err
		}
	}

	childWorkflowID := uuid.New().String()
	fmt.Printf("childWorkflowID %+v\n", childWorkflowID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID: childWorkflowID,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)

	var childOutput string
	err = workflow.ExecuteChildWorkflow(ctx, MainWorkflowChild, params).Get(ctx, &childOutput)
	if err != nil {
		return WorkflowResult{}, err
	}
	fmt.Printf("MainWorkflow childOutput: %+v\n", childOutput)

	results := WorkflowResult{OutputStep1: outputStep1, OutputStep2: outputStep2}
	return results, nil
}
