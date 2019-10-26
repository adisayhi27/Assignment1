from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import datetime
import time

"""Variable assigned for json source file path."""

path = r"C:\Users\Adi\PycharmProjects\HelloSparkCode\recipes.json"

"""Spark session initialization."""

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python_Spark_Code") \
    .getOrCreate()

def covert_time(pt):
    """Returns time in minutes."""
    #pt = pt.upper()
    a = pt.lstrip("PT")
    if "H" in a:
        t = a.split("H")
        if "M" not in a:
            t[1] = "0M"
    else:
        t = [0, a]
    return convert_hours_in_min(t[0]) + int(t[1].rstrip("M"))

def convert_hours_in_min(x):
    """Convert hours to minutes."""
    return int(x)*60

def add_time(preptime, cooktime):
    """Addition of prepTime & cookTime."""
    return preptime + cooktime

"""Converting Function to UDFs."""

time_cal_udf = udf(covert_time, IntegerType())
add_time_udf = udf(add_time, IntegerType())

"""Load json file into dataframe."""

df_load =spark.read.json(path)

"""Assign current datetime into a variable."""

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

"""Replace new line characters from these columns and select the columns in sequence."""

df_new_line_removed = df_load.withColumn("ingredients", regexp_replace('ingredients', "[\\r\\n]", "")) \
    .withColumn("recipeYield", regexp_replace('recipeYield', "[\\r\\n]", "")) \
    .withColumn("name", regexp_replace('name', "[\\r\\n]", "")) \
    .select("name","ingredients","url","image","cookTime","recipeYield","datePublished","prepTime","description")

# def replace_nL(ingred):
#     return ingred.replace("[\\r\\n]", "")

# ingredients_udf = udf(replace_nL, StringType())
# recipeYield_udf = udf(replace_nL, IntegerType())
# name_udf = udf(replace_nL, StringType())
#
# df_new_line_removed = df_load.withColumn("ingredients", ingredients_udf("ingredients")) \
#     .withColumn("recipeYield", recipeYield_udf("recipeYield")) \
#     .withColumn("name", name_udf("name")) \
#     .select("name","ingredients","url","image","cookTime","recipeYield","datePublished","prepTime","description")

"""Filter data on Column 'ingredients' by using 'Beef', by converting the column to uppercase for proper match. """

df_filter = df_new_line_removed.filter(F.upper(F.col("ingredients")).like("%BEEF%"))

"""Add columns in dataframe for prepTime and cookTime by converting into minutes."""

df_time = df_filter.withColumn('prepTimeMin', time_cal_udf('prepTime')).withColumn('cookTimeMin', time_cal_udf('cookTime'))

"""Add column 'date_of_execution' to track changes in the source data over time and a column having sum of prepTime and cookTime in dataframe.'"""

df_add_time = df_time.withColumn('date_of_execution',lit(timestamp)) \
    .withColumn('add_prep_cook_time', add_time_udf('prepTimeMin', 'cookTimeMin'))

"""Create a temp view after the above transformations."""

df_add_time.createOrReplaceTempView("df_final")

"""Add a column 'difficulty' based on prepTime and cookTime to the dataframe."""

df = spark.sql("""SELECT name,ingredients,url,image,cookTime,recipeYield,datePublished,prepTime,description,
CASE WHEN add_prep_cook_time > 60 THEN 'Hard'
WHEN add_prep_cook_time >=30 AND add_prep_cook_time <= 60 THEN 'Medium'
WHEN add_prep_cook_time < 30 THEN 'Easy' ELSE 'Unknown' END AS difficulty,
date_of_execution FROM df_final""")

#df.show(2)
"""Write the dataframe into parquet based on partition by 'difficulty' column."""

df.write.partitionBy("difficulty").parquet(path=r"C:\Users\Adi\PycharmProjects\HelloSparkCode\OutputFileParquet", mode="append")
#df.repartition(1).write.csv(path=r"C:\Users\Adi\PycharmProjects\HelloSparkCode\OutputFile", mode="overwrite", header="true")

"""Write the dataframe into impala table by making the connection."""

#df.write.mode("append").jdbc(url="jdbc:impala://192.168.10.555:21050/test;auth=noSasl",table="tempTable", prop="")

if __name__ == '__main__':
    print("Hello_Fresh_Code")