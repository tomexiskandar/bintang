/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [RowNumber]
      ,[Name]
      ,[ManufacturerName]
      ,[ModelNumber]
      ,[ModelName]
      ,[EntryID]
      ,[Cat]
  FROM [biomed].[dbo].[specs] WITH (nolock)
  where EntryID is not NULL;

  --updatex [biomed].[dbo].[specs] set EntryID = Null, Cat = Null