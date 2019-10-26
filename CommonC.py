from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import functools
import unittest
import smtplib
import configparser
#from email.message import EmailMessage
#from ConfigParser import SafeConfigParser
import datetime
import time
import os

"""Variable assigned for json source file path."""

path = r"C:\Users\Adi\PycharmProjects\HelloSparkCode\recipes.json"

"""Spark session initialization."""

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python_Spark_Code") \
    .getOrCreate()

"""Spark Configuration"""

#spark.sqlContext.setConf("spark.sql.shuffle.partitions", "100")
#spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

"""Reading Configuration File"""

configParser = configparser.RawConfigParser()
configFilePath = "C:/Users/Adi/PycharmProjects/HelloSparkCode/configuration.cfg"
configParser.read(configFilePath)
email_from = configParser.get('config-config', 'EMAIL_FROM')
email_to = configParser.get('config-config', 'EMAIL_TO')
email_subject = configParser.get('config-config', 'Mail_subject')
#impala_node = configParser.get('config-config', 'IMPALA_NODE')
#db_name = configParser.get('config-config', 'DB_NAME')

# """Sending Mail testing"""
def send_mail():
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login("aditya.101006@gmail.com", "Hari@2015")
    msg = """This is a test body"""
    s.sendmail(email_from, email_to, msg)
    s.quit()

send_mail()

def do_word_counts(lines):
    """ count of words in an rdd of lines """

    counts = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(lambda a, b: a + b)
             )
    results = {word: count for word, count in counts.collect()}
    return results

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

"""Test Case"""
# conf = SparkConf()
# sc = spark.sparkContext(conf=conf)
# sqlContext = HiveContext(sc)

# def test_file_load(self, df_load):
#     record_count = df_load.count
#     expected_count = 1040
#     self.assertEqual(record_count,expected_count)
#
# test_file_load(spark.sparkContext,df_load)

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

"""Write the final dataframe into table."""

#df.write.mode('append').saveAsTable('db_test.ingredients')

"""INVALIDATE Metadata is executed in impala for the table to reflect the changes done."""

#os.system("""impala-shell -i '""" + impala_node + """' -q 'compute stats db_test.ingredients'""")
#os.system("""impala-shell -i '""" + impala_node + """' -q 'invalidate metadata db_test.ingredients'""")

"""Job has been executed in local using the below command"""
#spark-submit --master local[1] --deploy-mode client C:\Users\Adi\PycharmProjects\HelloSparkCode\CommonC.py

if __name__ == '__main__':
    print("Hello_Fresh_Code")


class BaseTask(object):
    def __init__(self):
        self.path = r"C:\Users\Adi\PycharmProjects\HelloSparkCode\recipes.json"

    def initialise_spark_session(self):
        self.spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Python_Spark_Code") \
        .getOrCreate()

    def start(self):
        print("Started class ececution")

    def stop(self):
        print("Execution Finished")
        return False

    def handle_error(self, error):
        self.send_error_mail(error)
        self.stop()
        

    def send_error_mail(self,error):
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login("aditya.101006@gmail.com", "Hari@2015")
        msg = """This is a test body"""
        s.sendmail(email_from, email_to, msg)
        s.quit()

    def extract_new_lines(self):
        self.df_new_line_removed = self.df_load.withColumn("ingredients", regexp_replace('ingredients', "[\\r\\n]", "")) \
        .withColumn("recipeYield", regexp_replace('recipeYield', "[\\r\\n]", "")) \
        .withColumn("name", regexp_replace('name', "[\\r\\n]", "")) \
        .select("name","ingredients","url","image","cookTime","recipeYield","datePublished","prepTime","description")



class LoadData(BaseTask):

    def start(self):
        super(LoadData, self).start()
        try:
            self.initialise_spark_session()
            self.df_load =self.spark.read.json(self.path)
        except Exception as e:
            self.handle_error(e)


class Tranformation(LoadData):
    TRANSFORMATION_FUNCTIONS = [
        "extract_word",
        "add_difficulty",
        "add_doe",
        "stor_in_parquet",
    ]

    def start(self):
        super(Tranformation, self).start()
        try:
            for handler in TRANSFORMATION_FUNCTIONS:
                handler()
        except Exception as e:
            self.handle_error(e)

    def extract_word(self, word="beef"):
        self.extract_new_lines()
        self.df_filter = self.df_new_line_removed.filter(F.upper(F.col("ingredients")).like("%%s".format(word)))

    def add_difficulty(self):
        df_time = self.df_filter.withColumn('prepTimeMin', time_cal_udf('prepTime')).withColumn('cookTimeMin', time_cal_udf('cookTime'))
        df_add_time = df_time.withColumn('date_of_execution',lit(timestamp)) \
        .withColumn('add_prep_cook_time', add_time_udf('prepTimeMin', 'cookTimeMin'))
        df_add_time.createOrReplaceTempView("df_final")

        """Add a column 'difficulty' based on prepTime and cookTime to the dataframe."""
        self.df = spark.sql("""SELECT name,ingredients,url,image,cookTime,recipeYield,datePublished,prepTime,description,
        CASE WHEN add_prep_cook_time > 60 THEN 'Hard'
        WHEN add_prep_cook_time >=30 AND add_prep_cook_time <= 60 THEN 'Medium'
        WHEN add_prep_cook_time < 30 THEN 'Easy' ELSE 'Unknown' END AS difficulty,
        date_of_execution FROM df_final""")

    def store_to_parquet(self):
        self.df.write.partitionBy("difficulty").parquet(path=r"C:\Users\Adi\PycharmProjects\HelloSparkCode\OutputFileParquet", mode="append")


class AppendTable(Tranformation):
    def start(self):
        super(Tranformation, self).start()
        try:
            self.append_column()
        except Exception as e:
            self.handle_error(e)

    def append_column(self):
        self.df.write.mode('append').saveAsTable('db_test.ingredients')

        """INVALIDATE Metadata is executed in impala for the table to reflect the changes done."""

        # os.system("""impala-shell -i '""" + impala_node + """' -q 'compute stats db_test.ingredients'""")
        #os.system("""impala-shell -i '""" + impala_node + """' -q 'invalidate metadata db_test.ingredients'""")


class Executor(object):
    ARGS_CLASS_MAP = {
        "load_data": LoadData,
        "append_table": AppendTable,
        "transformation": Tranformation,
    }
    def __init__(self):
        self.execute()

    def execute(self):
        import sys
        arguments = sys.argv
        if arguments and len(arguments) == 1:
            try:
                cls = ARGS_CLASS_MAP[arguments]()
            except KeyError:
                print("Invalid Task. Please enter one of the following:",[k for k,v in  ARGS_CLASS_MAP.items()])
                return
            else:
                cls.start()
        else:
            cls = AppendTable()
            cls.start()