/****** Script for SelectTopNRows command from SSMS  ******/
SELECT RowNumber
      ,[Name] as [Spec Class]
      ,[ManufacturerName] as cstManufacturerTX
      ,[ModelNumber] as triModelNumTX
      ,[ModelName] as triModelNameTX
  FROM [biomed].[dbo].[specs]
  where RowNumber between 10001 and 20000
  order by RowNumber
	


