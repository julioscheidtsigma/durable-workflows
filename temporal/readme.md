### Temporal

Temporal has an API to process the workflows, and it provides an UI for visualization.
In order to run workflows, it needs a worker running and listening to the workflows and its activities. On the other end, it needs a client to trigger the execution, therefore in a server/client fashion.

The example scripts are creating a worker and a task starter.
The worker will register the workflow with `RegisterWorkflow` and its activities with `RegisterActivity`, and it will be listening to its events.
The client/task started will trigger the workflow with `ExecuteWorkflow`, and wait its response with `workflowRun.Get`.

How to run the worker and task starter:

```bash
go mod download

# worker
go run worker/main.go

# task - chosing which step to run
go run task/main.go <RUN_STEP>
e.g.
go run task/main.go [0,1,2]
```

Locally on a Mac, we can install and run both Temporal API and UI with this:

```bash
brew install temporal
temporal server start-dev --ui-port 8233
```

It will be listening on these ports:

- API: localhost:7233
- UI: http://localhost:8233
