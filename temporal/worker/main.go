package main

import (
	"log"

	"github.com/julioscheidtsigma/temporal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, workflow.QUEUE, worker.Options{
		MaxConcurrentActivityExecutionSize: 10,
		MaxConcurrentWorkflowTaskPollers:   2,
	})

	w.RegisterWorkflow(workflow.MainWorkflow)
	// w.RegisterActivity(workflow.FirstWorkflowStep)
	// w.RegisterActivity(workflow.SecondWorkflowStep)
	// dynamic activity registration
	w.RegisterActivity(&workflow.Activities{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
