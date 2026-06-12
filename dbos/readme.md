### DBOS

DBOS uses Postgres to store the workflows, steps, inputs/outputs, statuses, queues and so on. The main tables are:
- dbos.workflow_status
- dbos.operation_outputs

`workflow_status` stores the workflow uuid, name, status, inputs for all workflow, final output, queue used and some other fields.
The inputs are passed directly to the workflow as a whole, and they can be injected into the context to be accessed inside each step later on.

`operation_outputs` stores each step of the workflow, pointing to the workflow uuid, with the function name that was called for the step and each individual output. These entries are created as the steps are invoked.

`workflow_events` stores the events, pointing to the workflow uuid, and `workflow_events_history` stores each event with the key they were sent.

In order to run the workflows, it needs to register the workflow `dbos.RegisterWorkflow`, where the workflow will define its steps with `dbos.RunAsStep`. Triggering the workflow is done by `dbos.RunWorkflow`.

The execution of steps inside the workflow can be dynamic based on the workflow parameters, global values or output from previous steps inside the same workflow.

The example script `main.go` uses some of this features, like calling steps dynamically, injecting random failures into the steps to test the retries, queue and on-demand modes, inside a simple API.

How to run the API:

```bash
go mod download

go run main.go
```

Examples to call the API and execute the workflows:

```bash
# This will enqueue the workflow (`runAsQueue=true`), execute all steps (`runStep=0`), and just return the workflow was triggered.
curl -s -X GET "http://localhost:8585/workflow/start?urn=URN_001&runAsQueue=true&runStep=0"
# StartWorkflowHandler: workflow triggered successfully

# This will call the workflow immediately, execute all steps, and return the outputs from both steps.
curl -s -X GET "http://localhost:8585/workflow/start?urn=URN_001&runAsQueue=false&runStep=2"
# {"outputStep1":"","outputStep2":"SecondWorkflowStep succeeded"}

# This will call the workflow immediately, execute only the first step, and return the outputs from it.
curl -s -X GET "http://localhost:8585/workflow/start?urn=URN_001&runAsQueue=false&runStep=1"
# {"outputStep1":"FirstWorkflowStep succeeded","outputStep2":""}

# list workflows
curl -s -X GET "http://localhost:8585/workflow"

# rerun a workflow
curl -s -X GET "http://localhost:8585/workflow/rerun/16411325941583066502"
```

#### Docs

[https://docs.temporal.io/develop/go/set-up-your-local-go](https://docs.temporal.io/develop/go/set-up-your-local-go)

[https://docs.dbos.dev/golang/tutorials/workflow-communication#workflow-events](https://docs.dbos.dev/golang/tutorials/workflow-communication#workflow-events)

[https://docs.dbos.dev/production/checklist](https://docs.dbos.dev/production/checklist)

[https://github.com/dbos-inc/dbos-transact-golang#features](https://github.com/dbos-inc/dbos-transact-golang#features)

[https://docs.dbos.dev/python/examples/outbox](https://docs.dbos.dev/python/examples/outbox)

[https://docs.dbos.dev/python/examples/deploy-tracker-slackbot](https://docs.dbos.dev/python/examples/deploy-tracker-slackbot)

[https://docs.dbos.dev/python/examples/hacker-news-agent](https://docs.dbos.dev/python/examples/hacker-news-agent)
