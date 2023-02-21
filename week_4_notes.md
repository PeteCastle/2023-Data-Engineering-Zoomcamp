
### Differences from the original material (Synpapse)

```
CREATE TABLE [dbo].green_tripdata_combined
WITH   
  (   
    CLUSTERED COLUMNSTORE INDEX,  
    DISTRIBUTION = ROUND_ROBIN  
  )  
AS 
SELECT * FROM [dbo].[green_tripdata_2019]
UNION ALL
SELECT * FROM [dbo].[green_tripdata_2020]; 

CREATE TABLE [dbo].yellow_tripdata_combined
WITH   
  (   
    CLUSTERED COLUMNSTORE INDEX,  
    DISTRIBUTION = ROUND_ROBIN  
  )  
AS 
SELECT * FROM [dbo].[yellow_tripdata_2019]
UNION ALL
SELECT * FROM [dbo].[yellow_tripdata_2020]; 
```
* Data is loaded into the combined tables from separate 2019 and 2020 tables using the UNION ALL operator. 
* Unlike in the source material where the external table is already the combined data.




### Other notes
`dbt run --select +fact_trips`

* If there is a + before the name, it will also run dependencies of that SQL.