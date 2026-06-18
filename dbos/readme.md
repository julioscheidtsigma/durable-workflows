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
# This will enqueue the workflow, execute all steps (`step=0`), and just return the workflow was triggered
curl -s -X GET "http://localhost:8585/workflow/start?name=Donald%20Trump&runStep=0"
# StartWorkflowHandler: workflow triggered successfully

# This will enqueue the workflow, and execute only the second step
curl -s -X GET "http://localhost:8585/workflow/start?name=Donald%20Trump&runStep=2"
# StartWorkflowHandler: workflow triggered successfully

# list workflows
curl -s -X GET "http://localhost:8585/workflow"

# fork a workflow at specific step - changing the inputs
curl -s -X GET "http://localhost:8585/workflow/fork/39aa0077-5451-4329-8759-8b44abedd09e/start/3?name=Volodymyr%20Zelenskyy&runStep=4"

curl -s -X GET "http://localhost:8585/workflow/fork/cfa9fd8b-a795-4708-be01-37891bd767ca/start/0"


# change failure probability
curl -s -X GET "http://localhost:8585/failure?probability=0.0"
curl -s -X GET "http://localhost:8585/failure?probability=0.5"
curl -s -X GET "http://localhost:8585/failure?probability=1.0"

# crash testing
curl -s -X GET "http://localhost:8585/crash"
```

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
WITH recursive recursive_outputs as (
  select oo.workflow_uuid,
    oo.child_workflow_id,
    oo.function_name,
    -- oo.error,
    decode(oo.output, 'base64') as output,
    decode(ws.inputs, 'base64') as inputs,
    ((string_to_array(oo.child_workflow_id, '-'))[2]::int + 1) as child_global_level,
    oo.function_id as global_level,
    0 as local_level,
    to_timestamp(oo.started_at_epoch_ms/1000.0) at time zone 'UTC' as started_at,
    to_timestamp(oo.completed_at_epoch_ms/1000.0) at time zone 'UTC' as completed_at
  from dbos.operation_outputs oo
  join dbos.workflow_status ws on ws.workflow_uuid = oo.workflow_uuid
  where oo.function_name <> 'DBOS.setEvent' and
    oo.function_name <> 'DBOS.getResult' and
    oo.workflow_uuid = '3e48e041-5383-486c-88cd-5236a3033442'
union
  select o.workflow_uuid,
    o.child_workflow_id,
    o.function_name,
    -- o.error,
    decode(o.output, 'base64') as output,
    decode(ws.inputs, 'base64') as inputs,
    null as child_global_level,
    ((string_to_array(o.workflow_uuid, '-'))[2]::int + 1) as global_level,
    o.function_id as local_level,
    to_timestamp(o.started_at_epoch_ms/1000.0) at time zone 'UTC' as started_at,
    to_timestamp(o.completed_at_epoch_ms/1000.0) at time zone 'UTC' as completed_at
  from dbos.operation_outputs o
  join dbos.workflow_status ws on ws.workflow_uuid = o.workflow_uuid
  join recursive_outputs ro on ro.child_workflow_id = o.workflow_uuid
  where o.function_name <> 'DBOS.setEvent' and
    o.function_name <> 'DBOS.getResult'
)
select *
from recursive_outputs
order by global_level desc, local_level desc;


-- enqueue a workflow
SELECT dbos.enqueue_workflow(
    workflow_name => 'main.MainWorkflow',
    queue_name => 'edd-queue',
    positional_args => ARRAY[
        CAST('{"name":"Donald Trump","step":0}' AS json)
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
