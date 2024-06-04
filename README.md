# pyspark_postgres
docker image with python,pyspark,postgres,ssh


#####################

#comandi realizzazione

#####################

#build image

docker build --no-cache -t pyspark_postgres .

#run container

docker run -dt --name pyspark_postgres -v pspg_ps_data:/ps_data -v pspg_db_data:/var/lib/postgresql/data -p 1900:5432 -p 1901:22 pyspark_postgres

#####################

#credenziali

#####################

#postgres

hostname: localhost
porta: 1900
user: postgres
password: 

#ssh

hostname: localhost

porta: 1901

user: root

password: pyspark_postgres 


#####################

#volume pspg_ps_data

#####################

/ps_data : cartella condivisa principale

/ps_data/job : cartella degli script pyspark

/ps_data/output : cartella dei file in output

/ps_data/input : cartella dei file in input


#####################

#volume pspg_db_data

#####################

file di postgres
