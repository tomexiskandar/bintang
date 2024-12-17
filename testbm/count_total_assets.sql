SELECT a.Name, a.ManufacturerName, a.ModelNumber,  a.TotalAssets
       ,s.Cat, s.EntryID
FROM (
SELECT [Name]
      ,[ManufacturerName]
      ,[ModelNumber]
      --,[ModelName]

      ,count(BmeNumber) as TotalAssets
  FROM [biomed].[dbo].[assets]
  group by [Name]
      ,[ManufacturerName]
      ,[ModelNumber]
      --,[ModelName]
	  ) as a
INNER JOIN [biomed].[dbo].[specs] as s
ON  a.ManufacturerName=s.ManufacturerName and a.ModelNumber=s.ModelNumber
WHERE s.EntryID IS NOT NULL