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

# This will call the workflow immediately, execute only the second step, and return the outputs from both steps.
curl -s -X GET "http://localhost:8585/workflow/start?urn=URN_001&runAsQueue=false&runStep=2"
# {"outputDataCollection":"","outputEvidencesCollection":"SecondWorkflowStep succeeded"}

# This will call the workflow immediately, execute only the first step, and return the outputs from it.
curl -s -X GET "http://localhost:8585/workflow/start?urn=URN_001&runAsQueue=false&runStep=1"
# {"outputDataCollection":"FirstWorkflowStep succeeded","outputEvidencesCollection":""}

# list workflows
curl -s -X GET "http://localhost:8585/workflow"

# rerun a workflow
curl -s -X GET "http://localhost:8585/workflow/rerun/11940812423115777703/0"
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
  select workflow_uuid, child_workflow_id,
      function_name, decode(output, 'base64') as output,
      ((string_to_array(child_workflow_id, '-'))[2]::int + 1) as child_global_level,
      function_id as global_level,
      0 as local_level,
      to_timestamp(started_at_epoch_ms/1000.0) at time zone 'UTC' as started_at, to_timestamp(completed_at_epoch_ms/1000.0) at time zone 'UTC' as completed_at
    from dbos.operation_outputs
    where workflow_uuid = '?' and function_name <> 'DBOS.setEvent' and function_name <> 'DBOS.getResult'
  union
    select o.workflow_uuid, o.child_workflow_id,
      o.function_name, decode(o.output, 'base64') as output,
      null as child_global_level,
      ((string_to_array(o.workflow_uuid, '-'))[2]::int + 1) as global_level,
      o.function_id as local_level,
      to_timestamp(o.started_at_epoch_ms/1000.0) at time zone 'UTC' as started_at,
      to_timestamp(o.completed_at_epoch_ms/1000.0) at time zone 'UTC' as completed_at
    from dbos.operation_outputs o
    join recursive_outputs ro on ro.child_workflow_id = o.workflow_uuid
    where o.function_name <> 'DBOS.setEvent' and o.function_name <> 'DBOS.getResult'
)
select *
from recursive_outputs
order by global_level desc, local_level desc;


-- enqueue a workflow
SELECT dbos.enqueue_workflow(
    workflow_name => 'main.MainWorkflow',
    queue_name => 'edd-queue',
    positional_args => ARRAY[
        CAST('{"urn":"URN_001","runAsQueue":true,"runStep":1}' AS json)
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
