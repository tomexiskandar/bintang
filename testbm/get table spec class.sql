SELECT ROW_NUMBER() OVER(ORDER BY ManufacturerName, ModelNumber ASC) AS RowNumber
      ,Name, ManufacturerName, ModelNumber, ModelName, NULL as EntryID, NULL as Cat
INTO biomed.dbo.specs
FROM biomed.dbo.assets
WHERE ManufacturerName is not NULL and ManufacturerName not in ('-','**NULL**','_','0','NIL')
      AND ModelNumber is not NULL and ModelNumber not in ('TBA','NIL')
group by Name, ManufacturerName, ModelNumber, ModelName

CREATE CLUSTERED INDEX index1 ON biomed.dbo.specs (RowNumber);
     