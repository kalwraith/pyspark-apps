from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, ArrayType, StringType


spark=SparkSession()

schema = StructType([
    StructField('NAME', ArrayType(StringType()), True)
])

df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers','kafka01:9092,kafka02:9092,kafka03:9092') \
        .option('subscribe','test.spark.output-mode') \
        .option('maxOffsetsPerTrigger','1') \
        .load() \
        .selectExpr('CAST(key AS STRING) AS KEY',
                    'CAST(value AS STRING) AS VALUE') \
        .select(from_json(col('VALUE'), schema).alias('VALUE_JSON')) \
        .select(explode(col('VALUE_JSON.NAME')).alias('NAME')) \
        .groupBy('NAME').count() \

query = df.writeStream \
        .foreachBatch(for_each_batch_func) \
        .outputMode('update') \
        .option('checkpointLocation',self.chk_hdfs_dir) \
        .start()


query.awaitTermination()
