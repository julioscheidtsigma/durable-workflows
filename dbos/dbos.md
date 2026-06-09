### DBOS

DBOS uses postgres to store the workflows, steps, inputs/outputs, statuses, queues and so on. The main tables are:
- dbos.workflow_status
- dbos.operation_outputs

`workflow_status` stores the workflow uuid, name, status, inputs for all workflow, final output, queue used and some other fields.
The inputs are passed directly to the workflow as a whole, and they can be injected into the context to be accessed inside each step later on.

`operation_outputs` stores each step of the workflow, pointing to the workflow uuid, with the function name that was called for the step and each individual output. These entries are created as the steps are invoked.

The example script `main.go` uses some of this features, like calling steps dynamically, injecting random failures into the steps to test the retries, queue and on-demand modes, inside a simple API.

How to run the API:

```bash
go mod download

go run main.go
```

Examples to call the API and execute the workflows:

```bash
# This will enqueue the workflow (`runAsQueue=true`), execute all steps (`runStep=0`), and just return the workflow was triggered.
curl -s -X GET "http://localhost:8585/trigger/URN_001?runAsQueue=true&runStep=0"
# MainHandler: workflow triggered successfully

# This will call the workflow immediately, execute all steps, and return the outputs from both steps.
curl -s -X GET "http://localhost:8585/trigger/URN_001?runAsQueue=false&runStep=0"
# {FirstWorkflowStep succeeded SecondWorkflowStep succeeded}

# This will call the workflow immediately, execute only the first step, and return the outputs from it.
curl -s -X GET "http://localhost:8585/trigger/URN_001?runAsQueue=false&runStep=1"
# {FirstWorkflowStep succeeded}
```
