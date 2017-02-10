USE [master]
GO

/****** Object:  Database [ShiftJobsDB] ******/
CREATE DATABASE [ShiftJobsDB] 
GO

USE [ShiftJobsDB]
GO
/****** Object:  Table [dbo].[Job]  ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[Job](
	[JobID] [int] IDENTITY(1,1) NOT NULL,
	[AppID] [varchar](100) NULL,
	[UserID] [varchar](100) NULL,
	[ProcessID] [varchar](100) NULL,
	[JobType] [varchar](50) NULL,
	[JobName] [varchar](100) NULL,
	[InvokeMeta] [varchar](max) NULL,
	[Parameters] [varchar](max) NULL,
	[Command] [varchar](50) NULL,
	[Status] [int] NULL,
	[Error] [varchar](max) NULL,
	[Start] [datetime] NULL,
	[End] [datetime] NULL,
	[Created] [datetime] NULL,
	
	CONSTRAINT [PK_Job] PRIMARY KEY CLUSTERED 
	(
		[JobID] ASC
	)
)
GO

/****** Object:  Index [IX_ProcessID] ******/
CREATE NONCLUSTERED INDEX [IX_ProcessID] ON [dbo].[Job]
(
	[ProcessID] ASC
)
GO

/****** Object:  Table [dbo].[JobProgress] ******/
CREATE TABLE [dbo].[JobProgress](
	[JobID] [int] NOT NULL,
	[Percent] [int] NULL,
	[Note] [varchar](max) NULL,
	[Data] [varchar](max) NULL,

	CONSTRAINT [PK_JobProgress] PRIMARY KEY CLUSTERED 
	(
		[JobID] ASC
	)
) 
GO

SET ANSI_PADDING OFF
GO


/****** Object:  View [dbo].[JobView] ******/
CREATE VIEW [dbo].[JobView]
AS
SELECT dbo.Job.*, dbo.JobProgress.[Percent], dbo.JobProgress.Note, dbo.JobProgress.Data
FROM dbo.Job LEFT OUTER JOIN dbo.JobProgress ON dbo.Job.JobID = dbo.JobProgress.JobID

GO

