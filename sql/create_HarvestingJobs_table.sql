USE [DB_NAME]
GO

/****** Object:  Table [metadata].[HarvestingJobs]    Script Date: 09/01/2024 11:40:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [metadata].[HarvestingJobs](
	[snapshotId] [bigint] NOT NULL,
	[datasetName] [nvarchar](1000) NULL,
	[dateReleased] [datetime] NULL,
	[dataProviderCode] [varchar](100) NOT NULL,
	[dcrelease] [bit] NULL,
	[eurelease] [bit] NULL,
	[restrictFromPublic] [bit] NULL,
	[datasetId] [bigint] NOT NULL,
	[dataCollectionId] [bigint] NULL,
	[dataflowId] [bigint] NOT NULL,
	[recordLastModified] [datetime] NULL,
	[harvestDate] [datetime] NULL,
	[jobId] [int] NULL,
	[jobSummary] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

