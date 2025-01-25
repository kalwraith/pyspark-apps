from common.ch15_8.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, explode, window
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, TimestampType
from pyspark.sql import SparkSession


class Watermark(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)

    def main(self):
        schema = StructType([
            StructField('PERSON',ArrayType(
                StructType([
                    StructField('NAME', StringType(), True),
                    StructField('EVN_TS', TimestampType(), True)
                ]), True)
            )
        ])

        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers','kafka01:9092,kafka02:9092,kafka03:9092') \
                .option('subscribe','lesson.ch16-6.watermark') \
                .option('maxOffsetsPerTrigger','1') \
                .load() \
                .selectExpr('CAST(key AS STRING) AS KEY',
                            'CAST(value AS STRING) AS VALUE') \
                .select(from_json(col('VALUE'), schema).alias('VALUE_JSON')) \
                .select(explode(col('VALUE_JSON.PERSON')).alias('PERSON')) \
                .select(col('PERSON.*')) \
                .withWatermark('EVN_TS','1 hours') \
                .groupBy(                                           # 집계 함수는 writeStream 전달하기 전 수행되어야 함. 즉 writeStream 내 foreachBatch에서는 불가
                    window(col('EVN_TS'), '5 minutes'),
                    col('NAME')
                ).count()

        query = df.writeStream \
                .foreachBatch(lambda df, epoch: self.for_each_batch(df, epoch, spark)) \
                .outputMode('update') \
                .start()

        query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id: int, spark: SparkSession):
        df.orderBy('window').show(truncate=False)


if __name__ == '__main__':
    watermark = Watermark(app_name='watermark')
    watermark.main()