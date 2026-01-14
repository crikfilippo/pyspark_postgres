# Description

PySpark ETL in a docker container. 
Includes pyspark, a postgres db instance, python, ssh access.

# Building the image

docker build --no-cache -t pyspark_postgres .

# Running container

docker run -dt 
--name pyspark_postgres 
-v pspg_ps_data:/ps_data 
-v pspg_db_data:/var/lib/postgresql/data 
-p 1900:5432 
-p 1901:22 
pyspark_postgres

# Postgres Credentials

hostname: localhost  
port: 1900  
user: postgres  
password: 

# SSH Credentials

hostname: localhost  
port: 1901  
user: root  
password: pyspark_postgres 

# Volume `pspg_ps_data`

/ps_data : Main data folder  
/ps_data/job : PySpark scripts  
/ps_data/output : Output files  
/ps_data/input : Onput files

# Volume `pspg_db_data`

Postgres data
