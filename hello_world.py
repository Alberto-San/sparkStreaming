

# 0) Main repository: https://github.com/subhamkharwal/docker-images/tree/master/pyspark-jupyter-kafka
# 1) Create topic on kafka container 
# export KAFKKA_CONTAINER_URL=ed-kafka:9092
# export KAFKA_CHECK_TOPICS=kafka-topics --list --bootstrap-server $KAFKKA_CONTAINER_URL
# export KAFKA_CREATE_TOPIC=kafka-topics --create --topic test-topic --bootstrap-server $KAFKKA_CONTAINER_URL
# 2) Install "ncat"
# within jupyterlab container
# sudo apt-get update
# sudo apt-install ncat
# test ncat
# ncat -v 
# 3) Run pyspark script for streaming data
# 4) send data using command: ncat -l 9999
# 5) check output in spark session console

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, count, lit

class Monad:
    def __init__(self, value):
        self.value = value 
    
    def map(self, func):
        return Monad(
            func(self.value)
        )
    def for_each(self, func):
        return [func(value) for value in self.value]

def spark_session():
    master_url = "local[*]"
    spark = (
        SparkSession
        .builder
        .master(master_url)
        .getOrCreate()
    )
    return spark 

def jobflow(df_raw):
    df_words = df_raw.withColumns(
        "words", 
        split("value", " ")
    )
    df_explode = df_words.withColumn(
        "word",
        explode("words")
    )
    df_agg = df_explode.groupBy("word").agg(
        count(
            lit(1)
        ).alias("cnt")
    )
    return df_agg


def batch_jobflow(spark, path):
    df_agg = Monad(path).map(spark.read.format("text").load).map(jobflow)
    df_agg.show()

def show_stream(df):
    mode = "complete"
    df.writeStream.format("console")\
        .outputMode(mode)\
            .start().awaitTermination()

def streaming_jobflow(spark, config):
    Monad(
        spark.readStream.format("socket")
    ).map(
        lambda receiver: receiver.options(**config).load()
    ).map(
        jobflow
    ).map(
        show_stream
    )


config = {
    "host": "localhost",
    "port": "9999"
}
spark = spark_session()

Monad(config).map(
    lambda config: streaming_jobflow(spark, config)
)
