# Shift
Shift background or long running jobs into reliable and durable workers out of the main client app process. 

**Features:**
- Reliable and durable background / long running jobs.
- Out of band processing of long running jobs. 
- Ability to stop, reset, and restart long running jobs.
- Optional detailed progress tracking for each running jobs.
- Scale out with multiple shift managers to run large number of jobs.
- Optional encryption for serialized data.
- Run Shift Manager in your own .NET apps, Azure WebJob, or Windows services. 

**Shift Client**
The client add jobs and send commands to stop, delete, reset jobs, and force run.


**Shift Manager**
The manager component is part that executes commands from clients and run jobs. The manager is a simple .NET library and needs to run inside another app, Azure WebJob, or Windows service. When the manager runs a job, it creates one worker thread process per job. The manager tracks start/end date and time, commands from client, and processes clean-up. 

Each job can have these status:
- No status, ready to run
- Running
- Stopped
- Completed
- Error

Two runnable server apps are provided to get you up and running quickly. The Shift.WinService is the standalone windows service component, multiple Shift.WinService services can be installed in the same server. The Shift.WebJob is the Azure WebJob component that can be easily deployed to Azure cloud environment, multiple web jobs can also be deployed.
