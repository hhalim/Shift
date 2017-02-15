# Shift
Shift background or long running jobs into reliable and durable workers out of the client app process. Shift enables your app to easily run long running jobs in physically separate infrastructures. 

**Features:**
- Reliable and durable background and long running jobs.
- Out of band processing of long running jobs. 
- Ability to stop, reset, and restart long running jobs.
- Optional progress tracking for each running jobs.
- Scale out with multiple shift servers to run large number of jobs.
- Optional encryption for serialized data.
- Run Shift Server in your own .NET apps, Azure WebJobs, or Windows services. 
- Auto removal of older jobs.

The client component allows client apps to add jobs and send commands to Shift server to stop, delete, reset, and run jobs.

A simple example of adding a job:
```
var job = new TestJob();
var jobID = jobClient.Add(() => job.Start("Hello World"));
```

Add a long running job that report its progress:
```
var job = new TestJob();
var progress = new SynchronousProgress<ProgressInfo>();
var jobID = shiftClient.Add("Shift.Demo", () => job.Start("Hello World", progress));
```

The server component checks for available jobs through polling, using first-in, first-out (FIFO) queue method. The server is a simple .NET library and needs to run inside a .NET app, Azure WebJob, or Windows service. 

Two runnable server apps projects are included as quick start templates:
- [Shift.WinService](https://github.com/hhalim/Shift.WinService) is the standalone Windows service server component, multiple services can be installed in the same server. 
- [Shift.WebJob](https://github.com/hhalim/Shift.WebJob) is the Azure WebJob component that can be easily deployed to Azure cloud environment, multiple web jobs can also be deployed to multiple App Services. 

## Quick Start and More
Shift build package is on [nuget.org](https://www.nuget.org/packages/Shift), but first check out the [Shift.wiki](https://github.com/hhalim/Shift/wiki) for [Quick Start](https://github.com/hhalim/Shift/wiki/Quick-Start) and for more details. 

## Demos
- Console apps demo: [Shift.Demo.Client](https://github.com/hhalim/Shift.Demo.Client) and [Shift.Demo.Server](https://github.com/hhalim/Shift.Demo.Server)
- ASP.NET MVC demo: [Shift.Demo.MVC](https://github.com/hhalim/Shift.Demo.MVC)
