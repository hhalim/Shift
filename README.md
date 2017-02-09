# Shift
Shift background or long running jobs into reliable and durable workers out of the client app process. Shift enables your app to easily run long running jobs in physically separate infrastructures. 

**Features:**
- Reliable and durable background and long running jobs.
- Out of band processing of long running jobs. 
- Ability to stops, resets, and restarts long running jobs.
- Optional detailed progress tracking for each running jobs.
- Scale out with multiple shift servers to run large number of jobs.
- Optional encryption for serialized data.
- Run Shift Server in your own .NET apps, Azure WebJobs, or Windows services. 

The client component allows client apps to add jobs and send commands to Shift server to stop, delete, reset, and run jobs.

Adding jobs are as easy as using Linq lambda expression.
```
var job = new TestJob();
var jobID = jobClient.Add(() => job.Start("Hello World"));
```

Complex long running job that report progress can be added easily.
```
var job = new TestJob();
var progress = new SynchronousProgress<ProgressInfo>();
var jobID = jobClient.Add("Shift.Demo.Client", () => job.Start("Hello World", progress));
```

The server component gathers available jobs through polling. The server is a simple .NET library and needs to run inside a container app, Azure WebJob, or Windows service. 

Two runnable server apps projects are included as quick start templates:

The [Shift.WinService](https://github.com/hhalim/Shift.WinService) is the standalone Windows service server component, multiple services can be installed in the same server. 

The [Shift.WebJob](https://github.com/hhalim/Shift.WebJob) is the Azure WebJob component that can be easily deployed to Azure cloud environment, multiple web jobs can also be deployed to multiple App Services. If you're using Azure, it is highly recommended to locate the Azure SQL and Azure Redis within the same region as the web jobs.

## Quick Start and More
See the wiki for [[Quick Start]] and more details.

## Demos
- Console apps demo: [Shift.Demo.Client](https://github.com/hhalim/Shift.Demo.Client) and [Shift.Demo.Server](https://github.com/hhalim/Shift.Demo.Server)
- ASP.NET MVC demo: [Shift.Demo.MVC](https://github.com/hhalim/Shift.Demo.MVC)
