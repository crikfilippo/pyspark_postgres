# Description

docker image with python, pyspark, postgres, ssh 

# Build image

docker build --no-cache -t pyspark_postgres .

# Run container

docker run -dt --name pyspark_postgres -v pspg_ps_data:/ps_data -v pspg_db_data:/var/lib/postgresql/data -p 1900:5432 -p 1901:22 pyspark_postgres

# Postgres Credentials

hostname: localhost  
</br>port: 1900  
</br>user: postgres  
</br>password: 

# SSH Credentials

hostname: localhost  
</br>port: 1901  
</br>user: root  
</br>password: pyspark_postgres 

# Volume `pspg_ps_data`

/ps_data : main shared folder  
</br>/ps_data/job : folder for PySpark scripts  
</br>/ps_data/output : folder for output files  
</br>/ps_data/input : folder for input files

# Volume `pspg_db_data`

Postgres data files
