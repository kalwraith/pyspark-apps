from common.ch15_5.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql import SparkSession


class UpdateMode(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '2'
        self.log_mode = 'info'
        self.last_dttm = ''

    def main(self):
        schema = StructType([
            StructField('NAME', ArrayType(StringType()), True)
        ])

        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers','kafka01:9092,kafka02:9092,kafka03:9092') \
                .option('subscribe','lesson.ch16_4.output-mode') \
                .option('maxOffsetsPerTrigger','1') \
                .load() \
                .selectExpr('CAST(key AS STRING) AS KEY',
                            'CAST(value AS STRING) AS VALUE') \
                .select(from_json(col('VALUE'), schema).alias('VALUE_JSON')) \
                .select(explode(col('VALUE_JSON.NAME')).alias('NAME')) \
                .groupBy('NAME').count() \

        query = df.writeStream \
                .foreachBatch(lambda df, epoch: self.for_each_batch(df, epoch, spark)) \
                .outputMode('complete') \
                .start()

        query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id: int, spark: SparkSession):
        self.logger.write_log('info', 'Micro batch start', epoch_id)
        df.show(truncate=False)
        self.logger.write_log('info', 'Micro batch end', epoch_id)


if __name__ == '__main__':
    update_mode = UpdateMode(app_name='update_mode')
    update_mode.main()