
#import principali
import sys
import os

#parametri
output_path = "/ps_data/output/"
output_name = "test"
output_ext = ".txt"

# Importa librerie Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as PSF

#importa altre classi
sys.path.append(os.path.abspath("../classes"))
import df_s3_other_functions

# Crea sessione Spark
spark = SparkSession.builder.appName("DatasetProva").config("spark.jars","/venv/lib/postgresql-jdbc.jar") .getOrCreate()

#connetti a pg ed estrai
def table2df(table_name, database="postgres", user="postgres", password=""):
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/"+database).option("dbtable", table_name).option("user", user).option("password", password).option("driver", "org.postgresql.Driver").load()
    return df

# Crea un dataframe di prova con dati numerici e stringhe
data = [
    (1, "Valore1", 10.0),
    (2, "Valore2", 20.0),
    (3, "Valore3", 30.0),
]
columns = ["id", "nome", "valore"]
df = spark.createDataFrame(data,columns)

df = table2df("staging.test")

#etl
UOF = df_s3_other_functions.functions(dframe=df, imps={"ps_functions": PSF, "ps_types": None, "regex": None})
separator = PSF.lit("|")
df = df.select(PSF.concat(
        PSF.col("id").cast("string"),separator,
        PSF.col("nome").cast("string"),separator,
        PSF.col("valore").cast("string"),separator
).alias("value"))


#Salva il dataframe nella path specificata
output_df = df.coalesce(1)
output_df.write.mode('overwrite').text(output_path + output_name)

print("generato file di output sotto " + output_path + output_name)
