# Use Alpine Linux as the base image
FROM alpine:latest

# Install Python 3, Java, PostgreSQL, pip, nano, bash, and other necessary dependencies
RUN apk --no-cache add python3 openjdk11-jre postgresql py3-pip nano build-base python3-dev py3-numpy py3-six py3-wheel bash openssh curl

# Create a Python virtual environment and activate it
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Upgrade pip and install setuptools
RUN /venv/bin/pip install --upgrade pip setuptools

# Install PySpark within the virtual environment
RUN /venv/bin/pip install pyspark

# Configure PostgreSQL
RUN mkdir -p /run/postgresql/ && chown -R postgres:postgres /run/postgresql/

#download PostgreSQL driver
RUN curl -SL https://jdbc.postgresql.org/download/postgresql-42.3.4.jar -o /venv/lib/postgresql-jdbc.jar

# Configure SSH
RUN apk add --no-cache openssh \
  && ssh-keygen -A \
  && echo "root:pyspark_postgres" | chpasswd \
  && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

EXPOSE 22

USER postgres
RUN initdb -D /var/lib/postgresql/data
RUN echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf
RUN echo "listen_addresses='*'" >> /var/lib/postgresql/data/postgresql.conf
EXPOSE 5432

# Preconfigure PostgreSQL with databases and schemas
RUN pg_ctl -D /var/lib/postgresql/data start && \
  psql --command "ALTER USER postgres WITH PASSWORD 'pyspark_postgres';" && \
  psql --dbname=postgres --command "CREATE SCHEMA IF NOT EXISTS staging;" && \
  psql --dbname=postgres --command "CREATE SCHEMA IF NOT EXISTS production;" && \
  psql --dbname=postgres --command "CREATE SCHEMA IF NOT EXISTS domain_tables;" && \
  psql --dbname=postgres --command "CREATE SCHEMA IF NOT EXISTS public;" && \
  psql --command "ALTER USER postgres WITH SUPERUSER;" && \
  psql --dbname=postgres --command "GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;" && \
  psql --dbname=postgres --command "GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;" && \
  psql --dbname=postgres --command "GRANT ALL PRIVILEGES ON SCHEMA production TO postgres;" && \
  psql --dbname=postgres --command "GRANT ALL PRIVILEGES ON SCHEMA domain_tables TO postgres;" && \
  psql --dbname=postgres --command "GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;" && \
  psql --dbname=postgres --command "CREATE TABLE staging.test (  id int4 NOT NULL,  nome varchar NOT NULL,  valore varchar NOT NULL);" && \
  psql --dbname=postgres --command "INSERT INTO staging.test (id, nome, valore) VALUES (1, 'pippo', '10'), (2, 'pluto', '20'), (3, 'paperino', '30');"


USER root

# Environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Add Spark path to the PATH environment variable
ENV SPARK_HOME=/venv/lib/python3.12/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH

# Create the /ps_data directory structure with /jobs, /input, /output folders
RUN mkdir -p /ps_data/jobs /ps_data/input /ps_data/output

# Create the test.py script in the /ps_data/jobs directory
RUN echo -e "\n#parametri\noutput_path = \"/ps_data/output/\"\noutput_name = \"test\"\noutput_ext = \".txt\"\n\n# Importa librerie Spark\nfrom pyspark.sql import SparkSession\nfrom pyspark.sql.types import StringType\nimport pyspark.sql.functions as PSF\n\n# Crea sessione Spark\nspark = SparkSession.builder.appName(\"DatasetProva\").config(\"spark.jars\",\"/venv/lib/postgresql-jdbc.jar\") .getOrCreate()\n\n#connetti a pg ed estrai\ndef table2df(table_name, database=\"postgres\", user=\"postgres\", password=\"\"):\n    df = spark.read.format(\"jdbc\").option(\"url\", \"jdbc:postgresql://localhost:5432/\"+database).option(\"dbtable\", table_name).option(\"user\", user).option(\"password\", password).option(\"driver\", \"org.postgresql.Driver\").load()\n    return df\n\n# Crea un dataframe di prova con dati numerici e stringhe\ndata = [\n    (1, \"Valore1\", 10.0),\n    (2, \"Valore2\", 20.0),\n    (3, \"Valore3\", 30.0),\n]\ncolumns = [\"id\", \"nome\", \"valore\"]\ndf = spark.createDataFrame(data,columns)\n\ndf = table2df(\"staging.test\")\n\n#etl\nseparator = PSF.lit(\"|\")\ndf = df.select(PSF.concat(\n        PSF.col(\"id\").cast(\"string\"),separator,\n        PSF.col(\"nome\").cast(\"string\"),separator,\n        PSF.col(\"valore\").cast(\"string\"),separator\n).alias(\"value\"))\n\n\n#Salva il dataframe nella path specificata\noutput_df = df.coalesce(1)\noutput_df.write.mode('overwrite').text(output_path + output_name)\n\nprint(\"generato file di output sotto \" + output_path + output_name)" > /ps_data/jobs/test.py

# Automatically activate the Python virtual environment upon SSH login
RUN echo "source /venv/bin/activate" >> /etc/profile

# Start PostgreSQL and SSH, and keep them running
CMD ["sh", "-c", "su postgres -c 'pg_ctl -D /var/lib/postgresql/data start' && /usr/sbin/sshd && tail -f /dev/null"]
