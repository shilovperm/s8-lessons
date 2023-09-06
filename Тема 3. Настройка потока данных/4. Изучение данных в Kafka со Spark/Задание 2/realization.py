from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local")
        .appName('test connect to kafka')
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )
    
    return spark


def load_df(spark: SparkSession) -> DataFrame:
    df = (spark.read
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.sasl.jaas.config','org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
        .option("subscribe", "persist_topic")
        .load())
    return df

def transform(df: DataFrame) -> DataFrame:
    # определяем схему входного сообщения для JSON
    incomming_message_schema = StructType([StructField("subscription_id", IntegerType(), True),
        StructField("name" , StringType(), True),
        StructField("description" , StringType(), True),
        StructField("price" , DoubleType(), True),
        StructField("currency" , StringType(), True),
        StructField("key" , StringType(), True),
        StructField("value" , StringType(), True),
        StructField("topic" , StringType(), True),
        StructField("partition" , IntegerType(), True),
        StructField("offset" , LongType(), True),
        StructField("timestamp" , TimestampType(), True),
        StructField("timestampType" , IntegerType(), True)])
    
    transform_df = df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value"))
    transform_df = transform_df.select(transform_df.parsed_key_value.subscription_id.alias('subscription_id'),
                                   F.col('parsed_key_value.name').alias('name'),
                                   transform_df.parsed_key_value.price.alias('price'),
                                   transform_df.parsed_key_value.currency.alias('currency'),
                                   transform_df.parsed_key_value.key.alias('key'),
                                   transform_df.parsed_key_value.value.alias('value'),
                                   transform_df.parsed_key_value.topic.alias('topic'),
                                   transform_df.parsed_key_value.offset.alias('offset'),
                                   transform_df.parsed_key_value.timestamp.alias('timestamp'),
                                   transform_df.parsed_key_value.timestampType.alias('timestampType'))
    return transform_df


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)
