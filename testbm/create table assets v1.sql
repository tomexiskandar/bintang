USE [biomed]
GO
--drop table dbo.assets
-- truncate table dbo.assets

/****** Object:  Table [dbo].[assets]    Script Date: 30/06/2022 11:23:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[assets](
	[BmeNo] [nvarchar](20) NULL,
	[Name] [nvarchar](200) NULL,
	[Manufacturer] [nvarchar](200) NULL,
	[Brand] [nvarchar](100) NULL,
	[Model] [nvarchar](100) NULL,
	[ARTG] [nvarchar](30) NULL,
	[GmdnCode] [nvarchar](30) NULL,
	[SerialNo] [nvarchar](50) NULL,
	[AlternativeName] [nvarchar](500) NULL,
	[Supplier] [nvarchar](100) NULL,
	[ServiceAgent] [nvarchar](100) NULL,
	[SoftwareVersion] [nvarchar](100) NULL,
	[SiteName] [nvarchar](200) NULL,
	[MdName] [nvarchar](200) NULL,
	[LocationName] [nvarchar](200) NULL,
	[Price] [decimal](19, 2) NULL,
	[StartUpDate] [date] NULL,
	[DeliveryDate] [date] NULL,
	[Hospital] [nvarchar](200) NULL,
	[SubUnit] [nvarchar](200) NULL,
	[Filename] [nvarchar](100) NULL
) ON [PRIMARY]
GO


