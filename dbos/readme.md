### DBOS

DBOS uses Postgres to store the workflows, steps, inputs/outputs, statuses, queues and so on. The main tables are:
- dbos.workflow_status
- dbos.operation_outputs
- dbos.workflow_events
- dbos.workflow_events_history

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
# This will enqueue the workflow, and execute all modules
curl -s -X POST "http://localhost:8585/workflow" \
  -H 'content-type: application/json' \
  --data-raw '{"name": "Donald Trump", "runModules": 0}'
# This will enqueue the workflow, and execute only the first module
curl -s -X POST "http://localhost:8585/workflow" \
  -H 'Content-Type: application/json' \
  --data-raw '{"name": "Donald Trump", "runModules": 1}'

# multiple requests
for i in {1..10}; do
  curl -s -X POST "http://localhost:8585/workflow" \
    -H 'content-type: application/json' \
    --data-raw '{"name": "Donald Trump", "runModules": 0}' &
done

# list workflows
curl -s -X GET "http://localhost:8585/workflow"

# fork a workflow to start at a specific step - changing the inputs
curl -s -X POST "http://localhost:8585/workflow/f8e97ce5-4540-4f20-8b60-7a2f3dd1eedf/fork?startStep=3" \
  -H 'Content-Type: application/json' \
  --data-raw '{"name": "Volodymyr Zelenskyy", "runModules": 0}'
# fork a workflow to start at a specific step - same input - failed workflows
curl -s -X POST "http://localhost:8585/workflow/d0fe415d-36db-4521-822a-50cbca98ca35/fork?startStep=3"

# fork a workflow to run only a specific step - same input
curl -s -X POST "http://localhost:8585/workflow/95e051be-ffa0-4e46-88e7-e54c2e62f81b/fork?onlyStep=4" \
  -H 'Content-Type: application/json' \
  --data-raw '{"name": "Volodymyr Zelenskyy", "runModules": 0}'

# get exeuction graph
curl -s -X GET "http://localhost:8585/workflow/c48d9f7d-9588-4e5f-a13f-97ade7083484/graph"

# change failure probability
curl -s -X POST "http://localhost:8585/failure/injection?probability=0.0"
curl -s -X POST "http://localhost:8585/failure/injection?probability=0.5"
curl -s -X POST "http://localhost:8585/failure/injection?probability=1.0"

# crash testing
curl -s -X POST "http://localhost:8585/crash"
```

#### UI

Access the UI at to view the execution graph:
[http://localhost/dag.html?workflowUUID=<WORKFLOW_UUID>](http://localhost/dag.html?workflowUUID=<WORKFLOW_UUID>)

#### Docs

[https://docs.temporal.io/develop/go/set-up-your-local-go](https://docs.temporal.io/develop/go/set-up-your-local-go)

[https://docs.dbos.dev/golang/tutorials/workflow-communication#workflow-events](https://docs.dbos.dev/golang/tutorials/workflow-communication#workflow-events)

[https://docs.dbos.dev/production/checklist](https://docs.dbos.dev/production/checklist)

[https://github.com/dbos-inc/dbos-transact-golang#features](https://github.com/dbos-inc/dbos-transact-golang#features)

[https://docs.dbos.dev/python/examples/outbox](https://docs.dbos.dev/python/examples/outbox)

[https://docs.dbos.dev/python/examples/deploy-tracker-slackbot](https://docs.dbos.dev/python/examples/deploy-tracker-slackbot)

[https://docs.dbos.dev/python/examples/hacker-news-agent](https://docs.dbos.dev/python/examples/hacker-news-agent)

#### Useful queries

```sql
-- query to bring each step executed within the workflow
select
  oo.workflow_uuid,
  oo.function_name,
  (string_to_array(replace(oo.function_name, 'Level:', ''), ':'))[2]::text as step_name,
  (string_to_array(replace(oo.function_name, 'Level:', ''), ':'))[1]::int as global_level,
  oo.function_id as local_level,
  oo.output::json ->> 'status' as status,
  cast(CASE
      WHEN oo.serialization = 'portable_json' THEN oo.output
      ELSE decode(oo.output, 'base64')::text
  end as json) AS output,
  cast(CASE
      WHEN ws.serialization = 'portable_json' THEN ws.inputs
      ELSE decode(ws.inputs, 'base64')::text
  end as json) AS inputs,
  to_timestamp(oo.started_at_epoch_ms/1000.0) at time zone 'UTC' as started_at,
  to_timestamp(oo.completed_at_epoch_ms/1000.0) at time zone 'UTC' as completed_at
from dbos.operation_outputs oo
join dbos.workflow_status ws on ws.workflow_uuid = oo.workflow_uuid
where oo.workflow_uuid = ?
order by global_level asc, oo.started_at_epoch_ms asc;


-- enqueue a workflow
SELECT dbos.enqueue_workflow(
    workflow_name => 'main.MainWorkflow',
    queue_name => 'edd-queue',
    positional_args => ARRAY[
        CAST('{"name":"Donald Trump","runModules":0}' AS json)
    ],
    workflow_id => '?',
    priority => 1
);


-- analytics
WITH daily_workflows AS (
  SELECT
  DATE_TRUNC('day', TO_TIMESTAMP(created_at / 1000)) AS day,
  workflow_uuid
  FROM dbos.workflow_status
)
SELECT
  dw.day,
  COUNT(DISTINCT dw.workflow_uuid) AS workflow_count,
  COUNT(oo.workflow_uuid) AS step_count,
  COUNT(DISTINCT dw.workflow_uuid) + COUNT(oo.workflow_uuid) AS total_checkpoints
FROM daily_workflows dw
LEFT JOIN dbos.operation_outputs oo ON dw.workflow_uuid = oo.workflow_uuid
GROUP BY dw.day
ORDER BY dw.day DESC;
```
