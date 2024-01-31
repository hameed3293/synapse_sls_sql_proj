# Analyzing File Data using Synapse Serverless SQL Pool

### **Summary:**
This document illustrates the application of Synapse Serverless SQL pool in analyzing popular courses available on online learning platforms, focusing on subjects such as SQL, Data Factory, Databricks, etc. The primary dataset is sourced from a CSV file containing information about popular courses from Udemy and LinkedIn Learning, stored in an Azure Data Lake Gen2 Storage account. To access the data, we utilize the **'OPENROWSET' function**.

After accessing the data using the 'OPENROWSET' function, we create a database and implement schemas. Employing a multi-hop architecture, we will meticulously track the evolution of the data through the medallion architecture layer approach. This method ensures a comprehensive understanding of the data changes from inception to completion, offering valuable insights into the landscape of Data Engineering courses available on these platforms.

The csv file contains the following column fields:
- Content ID
- Content Title
- Brief Description
- Author
- Category
- Learning Platform
- Exepected Hrs Completion
- Platform Rating


## Sample Data for CSV File
Below is the table data we will be using:

| Content ID | Content Title                                              | Brief Description                          | Author            | Category                | Learning Platform | Expected Hrs Completion | Platform Rating |
|------------|------------------------------------------------------------|--------------------------------------------|-------------------|-------------------------|-------------------|-------------------------|-----------------|
| 1          | SQL for Data Analysis                                     | Intro-level course on T-SQL               | Travis Cuzick     | SQL                     | Udemy             | 6.5                     | 4.7             |
| 2          | The Advanced SQL Server Masterclass for Data Analysis     | Advance-level course on T-SQL             | Travis Cuzick     | SQL                     | Udemy             | 8                       | 4.7             |
| 3          | Microsoft Fabric                                          | Introduction to Microsoft Fabric           | Randy Minder      | Microsoft Fabric        | Udemy             | 12.5                    | 4.4             |
| 4          | Microsoft Fabric - A Deeper Dive                          | Deep Dive into Microsoft Fabric           | Randy Minder      | Microsoft Fabric        | Udemy             | 11                      | 4.5             |
| 5          | Azure Data Factory For Data Engineers                     | In-depth course on ADF                    | Ramesh Retnamy    | Azure Data Factory      | Udemy             | 13                      | 4.6             |
| 6          | Azure Synapse Analytics For Data Engineers                | In-depth course on Azure Synapse          | Ramesh Retnamy    | Azure Synapse Analytics | Udemy             | 13.5                    | 4.6             |
| 7          | Azure Databricks & Spark For Data Engineers               | In-depth on Azure Databricks              | Ramesh Retnamy    | Databricks              | Udemy             | 20                      | 4.7             |
| 8          | Master Power BI Desktop for Data Analysis                 | Best Seller Power BI Desktop               | Chris Dutton      | Power BI                | Udemy             | 15                      | 4.7             |
| 9          | Databricks Certified Data Engineer Associate - Preparation| Databricks DE Associate Training          | Derar Alhussein   | Certificate Training    | Udemy             | 4.5                     | 4.6             |
| 10         | Databricks Certified Data Engineer Professional - Preparation| Databricks DE Professional Training    | Derar Alhussein   | Certificate Training    | Udemy             | 3                       | 4.6             |
| 11         | Prepare for the Azure Data Engineer Associate Cert by Microsoft Press| In-depth review of DP-203 Exam Objectives | Tim Warner        | Certificate Training    | Linkedin Learning | 9.25                    | 4.7             |
| 12         | SQL Essential Training                                    | Introduction to SQL                        | Walter Shields    | SQL                     | Linkedin Learning | 4.26                    | 4.9             |
| 13         | Azure Data Factory                                        | Introduction to Azure Data Factory         | Karsen Ulferts    | Azure Data Factory      | Linkedin Learning | 1                       | 4.7             |
| 14         | Learning Microsoft Fabric                                | Intermediate training on Microsoft Fabric  | Helen Wall        | Microsoft Fabric        | Linkedin Learning | 1.27                    | 4.8             |
| 15         | Microsoft SQL Server 2022 Essential Training              | Training on SQL Server and T-SQL           | Adam Wilbert      | SQL Server              | Linkedin Learning | 5.23                    | 4.8             |
| 16         | SQL Server Integration Services                           | Training on SSIS and SQL Server           | Adam Wilbert      | SQL Server              | Linkedin Learning | 2.28                    | 4.7             |
| 17         | SQL Server Reporting Services                             | Training on SSRS and SQL Server           | Adam Wilbert      | SQL Server              | Linkedin Learning | 3.3                     | 4.8             |
| 18         | Introduction to Spark SQL and DataFrames                  | Training on Spark SQL/DataFrames          | Dan Sullivan      | SQL                     | Linkedin Learning | 1.53                    | 4.7             |
| 19         | Advanced SQL for Data Scientists                          | Advance SQL Training                       | Dan Sullivan      | SQL                     | Linkedin Learning | 2.3                     | 4.7             |
| 20         | Advanced SQL for Query Tuning and Performance Optimization| Advance SQL Training                       | Dan Sullivan      | SQL                     | Linkedin Learning | 2.9                     | 4.8             |


## PART 1: DATA DISCOVERY

### Viewing Data from CSV via Synapse
The **OPENROWSET** function gives us the ability to read remote files without having to worry about loading them into tables.  
**FYI: OPENROWSET will only work with Serverless SQL Pool, it is not supported for Dedicated SQL Pool.**  

We use the code snippet below to achieve this:

```sql
SELECT 
    *
FROM 
    OPENROWSET(
        BULK 'abfss://your_container_name@your_storage_gen2_account_name.dfs.core.windows.net/raw/data_engineering_training_courses.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
```

**NOTE**: By default Azure Synpase tends to use large table data types, which is not ideal if you intend on constantly rerunning the same query. 
It's best practice to set the data types for all your columns to optimize your query for better peformance.  

To view the default data types we use the stored procedure called **sp_describe_first_result_set** on our query:

```sql
EXEC sp_describe_first_result_set N'SELECT 
    *
FROM 
    OPENROWSET(
        BULK ''abfss://your_container_name@your_storage_gen2_account_name.dfs.core.windows.net/raw/data_engineering_training_courses.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS [result]'
```
### 

After viewing the results, proceed with setting up the data types for each of our columns using the WITH clause:

```sql
SELECT 
    *
FROM 
    OPENROWSET(
        BULK 'abfss://your_container_name@your_storage_gen2_account_name.dfs.core.windows.net/raw/data_engineering_training_courses.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) 
WITH (
    [Content ID] SMALLINT,
    [Content Title] VARCHAR(100),
    [Brief Description] VARCHAR(100),
    [Author] VARCHAR(25),
    [Category] VARCHAR(40),
    [Learning Platform] VARCHAR(20),
    [Expected Hrs Completion] FLOAT,
    [Platform Rating] FLOAT
) AS [result]

```

**NOTE**: By default Synpase apply a conversion to UTF-8 characters. To keep our UTF-8 characters as they are, we have to apply a UTF-8 collation. Now by default you cannot apply a collation change on the **master** database. 
Thus we will create a new database called **content_training_discovery** an apply the collation change there.  

Here is the snippet we use to create our new database and apply the UTF-8 collation: 

```sql
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'content_training_discovery')
    CREATE DATABASE content_training_discovery;

USE content_training_discovery;
ALTER DATABASE content_training_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;
```

Lastly for this section we will construct an **External Data Source**. External data sources are synapse objects that you can create in synapse pointing to the external data source such as Azure Data Lake Storage Gen2.
In short it will hide the URL so we do not have to explicitly write it in query for our execution runs. Below is the code snippet use for external data source creation:

```sql
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'enter_name_raw')
    CREATE EXTERNAL DATA SOURCE project_raw 
    WITH (
        LOCATION = 'abfss://your_container_name@your_storage_gen2_account_name.dfs.core.windows.net/raw'
    );

```

Now we can proceed with using the ***DATA_SOURCE*** option in the OPENROWSET Function:

```sql
-- Now run our query using DATA_SOURCE:
SELECT 
    *
FROM 
    OPENROWSET(
        BULK 'data_engineering_training_courses.csv.csv',
        DATA_SOURCE = ''enter_name_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) 
WITH (
    [Content ID] SMALLINT,
    [Content Title] VARCHAR(100),
    [Brief Description] VARCHAR(100),
    [Author] VARCHAR(25),
    [Category] VARCHAR(40),
    [Learning Platform] VARCHAR(20),
    [Expected Hrs Completion] FLOAT,
    [Platform Rating] FLOAT
) AS [result]
```


## Part 2: Presetup for External Tables
We're going to implement a logical data warehouse and setup the following to create our external tables:
- Database Schema
- External File Format
- External Data Source


**Database and Schema snippet**:
```sql
USE master
GO 

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'project_ldw')
    CREATE DATABASE project_ldw
GO 

ALTER DATABASE project_ldw COLLATE Latin1_General_100_BIN2_UTF8 
GO 

USE project_ldw 
GO 

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze')
GO 

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver')
GO 

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold')
GO 

```

**External File Format snippet**:
```sql
USE project_ldw;

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name='csv_file_format')
    CREATE EXTERNAL FILE FORMAT csv_file_format 
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ','
        ,STRING_DELIMITER = '"'
        ,FIRST_ROW = 2
        ,USE_TYPE_DEFAULT = FALSE
        ,ENCODING = 'UTF8'
        ,PARSER_VERSION = '2.0')
    );


IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_file_format')
    CREATE EXTERNAL FILE FORMAT parquet_file_format 
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
```

**External Data Source snippet**:
```sql
USE project_ldw;

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'project_src')
    CREATE EXTERNAL DATA SOURCE project_src
    WITH
    (
        LOCATION = 'https://your_storage_gen2_account_name.dfs.core.windows.net/project'
    );
```


## Part 3: Creating External Tables
We'll now proceed with creating our external tables. These tables can be acccessed like any other tables in a relation database and we can use BI tools to build reports from these tables.  

We will construct the following external tables:
- Bronze
- Silver
- Gold


**Bronze Table snippet**:
```sql
USE project_ldw;

IF OBJECT_ID('bronze.content_training') IS NOT NULL 
    DROP EXTERNAL TABLE bronze.content_training

CREATE EXTERNAL TABLE bronze.content_training
(
    [Content ID] SMALLINT,
    [Content Title] VARCHAR(100),
    [Brief Description] VARCHAR(100),
    [Author] VARCHAR(25),
    [Category] VARCHAR(40),
    [Learning Platform] VARCHAR(20),
    [Expected Hrs Completion] FLOAT,
    [Platform Rating] FLOAT
) WITH (
    LOCATION = 'raw/data_engineering_training_courses.csv',
    DATA_SOURCE = project_src,
    FILE_FORMAT = csv_file_format
);

```

For the Silver table, we'll apply a brief transformation by renaming some of the column names.  
**Silver Table Snippet**:
```sql
USE project_ldw;

IF OBJECT_ID('silver.content_training') IS NOT NULL 
    DROP EXTERNAL TABLE silver.content_training;

CREATE EXTERNAL TABLE silver.content_training
    WITH 
    (
        DATA_SOURCE = project_src, 
        LOCATION = 'silver/processed',
        FILE_FORMAT = parquet_file_format 
    )
AS 
SELECT 
    [Content_ID] = [Content ID]
   ,[Content_Title] = [Content Title]
   ,[Brief_Description] = [Brief Description]
   ,Author
   ,Category
   ,[Learning_Platform] = [Learning Platform]
   ,[Expected_Hrs_Completion] = [Expected Hrs Completion]
   ,[Platform_Rating] = [Platform Rating]
FROM
    bronze.content_training;


SELECT * FROM silver.content_training;
```

For the gold table creation, we proceed with applying a few aggregate functions to answer the following questions:
- How many courses does each author have?
- Which is consider the best ranking course for a given category?

For the gold table construction, I implementated this as a stored procedure. 

**Gold Table Stored Procedure Snippet**:
```sql
USE project_ldw
GO 

CREATE OR ALTER PROCEDURE gold.de_content_training
AS
BEGIN

    DECLARE @drop_sql_stmt NVARCHAR(MAX),
            @create_sql_stmt NVARCHAR(MAX);
    
    SET @drop_sql_stmt = 
    'IF OBJECT_ID(''gold.content_training'') IS NOT NULL 
        DROP EXTERNAL TABLE gold.content_training';

    print(@drop_sql_stmt)
    EXEC sp_executesql @drop_sql_stmt;

    SET @create_sql_stmt = 
        'CREATE EXTERNAL TABLE gold.content_training
         WITH 
            (
                DATA_SOURCE = project_src, 
                LOCATION = ''gold/presentation'',
                FILE_FORMAT = parquet_file_format 
            )
         AS 
         SELECT 
             Content_Title, 
             Author, 
             Category,
             Learning_Platform,
             [Number_of_Courses_By_Author] = COUNT(Author) OVER(PARTITION BY Author),
             Platform_Rating,
             [Platform_Rating Rank] = ROW_NUMBER() OVER(PARTITION BY Category ORDER BY Platform_Rating),
             [Platform_Rating Rank With Rank] = RANK() OVER(PARTITION BY Category ORDER BY Platform_Rating),
             [Platform_Rating Rank With Dense Rank] = DENSE_RANK() OVER(PARTITION BY Category ORDER BY Platform_Rating)
         FROM silver.content_training';

    print(@create_sql_stmt)
    EXEC sp_executesql @create_sql_stmt;
END;
```

To execute the stored procedure use this snippet:

```sql
USE project_ldw;

EXEC gold.de_content_training;

SELECT *
FROM gold.content_training;
```
