USE [master]
GO

/****** Object:  Database [BGProcess]    Script Date: 11/19/2015 4:23:57 PM ******/
CREATE DATABASE [BGProcess] ON  PRIMARY 
( NAME = N'BGProcess', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\DATA\BGProcess.mdf' , SIZE = 4096KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'BGProcess_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\DATA\BGProcess_log.ldf' , SIZE = 1024KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO


USE [BGProcess]
GO
/****** Object:  Table [dbo].[Job]    Script Date: 12/4/2015 2:46:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[Job](
	[JobID] [int] IDENTITY(1,1) NOT NULL,
	[AppID] [varchar](100) NULL,
	[UserID] [int] NULL,
	[ProcessID] [int] NULL,
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

SET ANSI_PADDING OFF
GO


/****** Object:  Table [dbo].[JobProgress]    Script Date: 12/4/2015 2:47:12 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

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

/****** Object:  Table [dbo].[JobResult]    Script Date: 1/21/2016 4:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[JobResult](
	[JobResultID] [int] IDENTITY(1,1) NOT NULL,
	[JobID] [int] NOT NULL,
	[ExternalID] [varchar](32) NULL,
	[Name] [varchar](250) NULL,
	[BinaryContent] [varbinary](max) NULL,
	[ContentType] [varchar](50) NULL,
	
	CONSTRAINT [PK_JobResult] PRIMARY KEY CLUSTERED 
	(
		[JobResultID] ASC
	)
)

GO

SET ANSI_PADDING OFF
GO



/****** Object:  View [dbo].[JobView]    Script Date: 12/4/2015 2:52:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[JobView]
AS
SELECT        dbo.Job.*, dbo.JobProgress.[Percent], dbo.JobProgress.Note, dbo.JobProgress.Data
FROM            dbo.Job LEFT OUTER JOIN
                         dbo.JobProgress ON dbo.Job.JobID = dbo.JobProgress.JobID

GO

