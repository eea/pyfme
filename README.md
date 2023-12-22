# rn3: Python tools to help manage EEA Reportnet3 

This repository contains tools to ineract with Reportnet3.

## Requirements
* Windows or Linux operating system
* Python x64 3.8 - 3.12

## Installation

From PyPI: 

`pip install rn3`


## <u>**Use 1**</u>: Generate Microsoft SQL script to create data tables based on a dataset in Reportnet3

```
import rn3

ds = DatasetModel()
ds.from_url(
    dataset_id=20822,
    api_key="ApiKey d79237c1-8942-44b9-b6df-2ef20fca66a4",
    base_url=r"https://sandbox-api.reportnet.europa.eu",
)
sql_cmd = ds.sql_cmd(self, database_name="EnergyCommunity", schema_name="annex_XXIV")

print(sql_cmd)
```

output: 

```
USE [EnergyCommunity]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [annex_XXIV].[PaMs](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Title] [nvarchar](500) NOT NULL,
	[TitleNational] [nvarchar](500) NULL,
	[IsGroup] [int] NOT NULL,
	[ListOfSinglePams] [nvarchar](500) NULL,
	[ShortDescription] [text] NOT NULL,
	[ReportNet3HistoricReleaseId] [int] NOT NULL,
CONSTRAINT [PK_PaMs] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [annex_XXIV].[PaMs]  WITH NOCHECK ADD  CONSTRAINT [FK_PaMs_ReportNet3HistoricReleases] FOREIGN KEY([ReportNet3HistoricReleaseId])
REFERENCES [metadata].[ReportNet3HistoricReleases] ([Id])
ON DELETE CASCADE
GO
ALTER TABLE [annex_XXIV].[PaMs] CHECK CONSTRAINT [FK_PaMs_ReportNet3HistoricReleases]
GO

.
.
.

... for all tables in dataset

```

