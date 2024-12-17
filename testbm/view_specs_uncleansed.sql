/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [RowNumber]
      ,[Name]
      ,[ManufacturerName]
      ,[ModelNumber]
      ,[ModelName]
      ,[EntryID]
      ,[Cat]
	  , IsMatched = CASE WHEN EntryID is not Null then 'Y' Else 'N' END
	  , CatDesc = CASE Cat WHEN 1 THEN '1 all matched'
	                       WHEN 2 THEN '2 Manuf ModelNum matched'
						   WHEN 3 THEN '3 Name ModelNum matched'
						   WHEN 4 THEN '4 Modelnum matched'
				   END
      , 1 as RecordCount
  FROM [biomed].[dbo].[specs]
  WHERE Cat is not NULL
