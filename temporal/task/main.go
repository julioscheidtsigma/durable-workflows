package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/julioscheidtsigma/temporal/workflow"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func main() {
	ctx := context.Background()

	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	urn := "URN_001"

	runStep := workflow.RUN_STEP_0 // default to run all steps
	if len(os.Args) > 1 {
		runStepStr := os.Args[1] // get runStep from command line argument
		switch runStepStr {
		case "0":
			runStep = workflow.RUN_STEP_0
		case "1":
			runStep = workflow.RUN_STEP_1
		case "2":
			runStep = workflow.RUN_STEP_2
		default:
		}
	}

	params := workflow.WorkflowParams{
		URN:     urn,
		RunStep: runStep,
	}
	fmt.Printf("params %+v\n", params)

	// the idempotency key will be used as workflow id
	idempotencyKey := params.GetIdempotencyKey()
	fmt.Printf("idempotencyKey %+v\n", idempotencyKey)

	options := client.StartWorkflowOptions{
		ID:                       idempotencyKey,
		TaskQueue:                workflow.QUEUE,
		WorkflowExecutionTimeout: time.Minute * 5,
		WorkflowRunTimeout:       time.Minute * 5,
		WorkflowTaskTimeout:      time.Minute * 1,
		// Allow starting a workflow execution using the same workflow id, only when the last
		// execution's final state is one of [terminated, cancelled, timed out, failed].
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		// Don't start a new workflow; instead return a workflow handle for the running workflow.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}

	// workflowRun, err := c.ExecuteWorkflow(ctx, options, "MainWorkflow", params)
	workflowRun, err := c.ExecuteWorkflow(ctx, options, workflow.MainWorkflow, params)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", workflowRun.GetID(), "RunID", workflowRun.GetRunID())

	var result workflow.WorkflowResult
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}

	log.Printf("result: %+v\n", result)
}
