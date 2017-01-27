# Shift
Shift background or long running jobs into reliable and durable workers out of the main client app process. 

*Benefits:*
- Reliable and durable background / long running jobs.
- Out of band processing of long running jobs. 
- Ability to stop, reset, and restart long running jobs.
- Optional detailed progress tracking for each running jobs.
- Scale with multiple workers and servers to run large number of jobs.
- Optional encryption for serialized data.
- Run workers as Azure web jobs, Windows services, or in your own apps. 

*Shift Client*
The client add jobs and send commands to stop, delete, reset jobs

*Shift Server*
The server component is the manager that executes commands from clients and run jobs.
This component can run automatically to gather available jobs ready to run, it creates one worker process when running each jobs. The worker process tracks start/end date and time, command from client, and basic statuses. 
Each worker have these statutes:
- No status, ready to run
- Running
- Stopped
- Completed
- Error
