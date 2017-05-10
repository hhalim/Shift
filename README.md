# Shift
Shift background or long running jobs into reliable and durable workers out of your main application. Shift enables your application to easily run long running jobs in separate infrastructures. 

**Features:**
- Open source and free - including commercial use.
- Reliable and durable background and long running jobs.
- Out of band processing of long running jobs. 
- Ability to stop, reset, and restart long running jobs.
- Auto removal of older jobs.
- Optional progress tracking for each running jobs.
- Scale out with multiple shift servers to run large number of jobs.
- Optional encryption for serialized data.
- Redis persistent storage by default. Other supported storage: MongoDB, Microsoft SQL server, Azure DocumentDB. 
- Run Shift Server in your own .NET apps, Azure WebJobs, or Windows services. Check out the [Shift.WinService](https://github.com/hhalim/Shift.WinService) and [Shift.WebJob](https://github.com/hhalim/Shift.WebJob) projects.

The client component allows client apps to add jobs and send commands to Shift server to stop, delete, reset, and run jobs.

A simple example of adding a job:
```
var job = new TestJob();
var jobID = jobClient.Add(() => job.Start("Hello World"));
```

Add a long running job that periodically reports its progress:
```
var job = new TestJob();
var progress = new SynchronousProgress<ProgressInfo>();
var jobID = shiftClient.Add("Shift.Demo", () => job.Start("Hello World", progress));
```

The server component checks for available jobs through polling, using first-in, first-out (FIFO) queue method. The server is a simple .NET library and needs to run inside a .NET app, Azure WebJob, or Windows service. 

Two deployable and runnable server apps projects are also provided as a starting point:
- [Shift.WinService](https://github.com/hhalim/Shift.WinService) is the standalone Windows service server component, multiple services can be installed in the same server. 
- [Shift.WebJob](https://github.com/hhalim/Shift.WebJob) is the Azure WebJob component that can be easily deployed to Azure cloud environment, multiple web jobs can also be deployed to multiple App Services. 

## Demos
Please check out the demo apps first to provide better understanding on how to integrate Shift into your own .NET application. There is the ASP.NET MVC demo that shows Shift client and server running in the same ASP.Net process, and the simpler console Shift client and server apps demo. The console apps are two separate projects that demonstrate the client and the server working in two different processes.
- ASP.NET MVC demo: [Shift.Demo.Mvc](https://github.com/hhalim/Shift.Demo.Mvc)
- ASP.NET Core MVC demo: [Shift.Demo.Mvc.Core](https://github.com/hhalim/Shift.Demo.Mvc.Core)
- Console apps demo: [Shift.Demo.Client](https://github.com/hhalim/Shift.Demo.Client) and [Shift.Demo.Server](https://github.com/hhalim/Shift.Demo.Server)

## Quick Start and More
Shift package is on [nuget.org](https://www.nuget.org/packages/Shift), but first check out the Shift wiki for [Quick Start](https://github.com/hhalim/Shift/wiki/Quick-Start), [Scheduling, Batch and Continuation jobs](https://github.com/hhalim/Shift/wiki/Schedule-Batch-Continuation), [Message Queuing](https://github.com/hhalim/Shift/wiki/Message-Queuing), and more. 

## Credits
Shift uses the following open source projects:
- [Autofac](http://autofac.org/)
- [Dapper](https://github.com/StackExchange/Dapper)
- [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)
- [Json.NET](http://james.newtonking.com/json)
- [MongoDB C# Driver](https://github.com/mongodb/mongo-csharp-driver)
- [Azure DocumentDB .NET SDK](https://github.com/Azure/azure-documentdb-dotnet)

