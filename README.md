# Pyspark ETL
--- 
ETL job creates a data lake from unstructured data from a ficticious music app, containing song data and user log data. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs from the app.

## Purpose of the project
Build an ETL pipeline for a data lake hosted on S3, with the following steps:
* Load JSON data from S3
* Process the data using Spark to create tables
* Load them back into an AWS hosted S3 service with partitioned write


## Pyspark table Structure:
### Final schema. 
The final schema follows the [star schema](https://en.wikipedia.org/wiki/Star_schema) principle. The tables are listed below:

### Fact tables:

#### songplays - colname, datatype, constraint:
* start_time 
* year
* month
* user_id 
* level 
* song_id 
* artist_id 
* session_id 
* location 
* user_agent 

### Dimension tables:

#### users - colname, datatype, constraint:
* user_id
* first_name
* last_name 
* gender 
* level

#### songs  - colname, datatype, constraint:
* song_id 
* title
* artist_id
* year
* duration

#### artists - colname, datatype, constraint:
* artist_id
* name
* location
* latitude
* longitude

#### time - colname, datatype, constraint:
* start_time
* hour
* day
* week
* month
* year
* weekday

## Code:

### Functional content
* etl.py - copies json data from s3 buckets into pyspark, transforms and loads into the final (star) schema back into S3. 
* df.cfg - location of configuaration details for AWS.

## How to use:
* ensure pyspark (& JVM) is installed.
* create an S3 endpoint, noting the **endpoint** (no endpoint currently exists)
* open df.cfg and overwrite AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY with the relevant details.
* in a terminal, run `python etl.py`
