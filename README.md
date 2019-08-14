# oracle_snowflake_etl
ETL utility to Extract data from Oracle and Load to Snowflake

# Available Arguments
  --batchCount  (No Of Batches:Determines how many batches will be created)
  
  --maxBatchSize (Maximum Records in a Single DB Fetch)
  
  --filePrefix   (Output file Name Prefix)
  
  --configFile   (Configuration file Path)
  
  --snowflakeTable (Snowflake Table to Load)
  
  --splitQuery  (Oracle Query to Determine the ranges)
  
  --splitColumnType (Data Type of Split Column can be NUMBER or DATE (This is Optional Parameter))
  
  --query (Oracle Query to Execute)
  

# Sample Config file

  oracle:
  
    db: <oracle db tns connect string>
    
    userName: <schema>
    
    password: <password>
    
  output:
  
    inbox: output/inbox/
    
    work:  output/work/ 
    
  snowflake:
  
    account: <account>
    
    database: <db>
    
    schema: <schema>
    
    user: <user>
    
    password: <password>
    
    warehouse: <warehouse>
    
