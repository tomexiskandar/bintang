USE [biomed]
GO

/****** Object:  Table [dbo].[assets]    Script Date: 24/06/2022 2:52:25 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[assets]') AND type in (N'U'))
DROP TABLE [dbo].[assets_temp]
GO

/****** Object:  Table [dbo].[assets]    Script Date: 24/06/2022 2:52:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[assets_temp](
	[Asset No] [nvarchar](20) NULL,
	[id] [nvarchar](200) NULL,
	[Name] [nvarchar](200) NULL,
	[Manufacturer] [nvarchar](200) NULL,
	[Model] [nvarchar](100) NULL,
	[Serial No] [nvarchar](50) NULL,
	[Alternative Name] [nvarchar](500) NULL,
	[Supplier] [nvarchar](100) NULL,
	[Site Name] [nvarchar](200) NULL,
	[MD Name] [nvarchar](200) NULL,
	[Name of Location] [nvarchar](200) NULL,
	[Price] decimal(19,2) NULL,
	[Start up] date NULL,
	[Filename] [nvarchar](100) NULL
) ON [PRIMARY]
GO


