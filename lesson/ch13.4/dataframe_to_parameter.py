from common.base_stream_app import BaseStreamApp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType

class RtBicycleRent(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '2'

    def main(self):
        # sparkSession 객체 얻기
        # 만약 다른 parameter를 추가하고 싶다면 self.get_session_builder() 뒤에 .config()을 사용하여 파라미터를 추가하고 getOrCreate 합니다.
        spark = self.get_session_builder().getOrCreate()

        # rslt_df 데이터프레임 공유하기
        global rslt_df
        rslt_df = spark.createDataFrame([(None,None,None,None),],'STT_ID STRING, BASE_DT STRING, RENT_CNT INT, RETURN_CNT INT')

        streaming_query = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
            .option("subscribe", "lesson.spark-streaming.rslt-sample") \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr(
                "CAST(key AS STRING) AS KEY",
                "CAST(value AS STRING) AS VALUE"
             ) \
            .select(
                 get_json_object(col('VALUE'),'$.STT_ID').alias('STT_ID')
                ,get_json_object(col('VALUE'),'$.BASE_DT').alias('BASE_DT')
                ,get_json_object(col('VALUE'), '$.RENT_CNT').cast(IntegerType()).alias('RENT_CNT')
                ,get_json_object(col('VALUE'), '$.RETURN_CNT').cast(IntegerType()).alias('RETURN_CNT')
             ) \
            .writeStream \
            .foreachBatch(self.for_each_batch) \
            .option("checkpointLocation", self.kafka_offset_dir) \
            .start()
        streaming_query.awaitTermination()

    def _for_each_batch(self, df: DataFrame, epoch_id):
        global rslt_df
        self.logger.write_log('info', 'Micro batch start', epoch_id)

        rslt_df = self.update_status(df, rslt_df)

        self.logger.write_log('info','rslt_df.show()',epoch_id)
        rslt_df.show()
        self.logger.write_log('info', 'Micro batch end', epoch_id)

    def update_status(self, new_df:DataFrame, total_df: DataFrame):
        return_df = total_df.alias('r').join(
            other   = new_df.alias('i'),
            on      = ['STT_ID', 'BASE_DT'],
            how     = 'full'
        ).selectExpr(
            'CASE WHEN r.STT_ID IS NULL THEN i.STT_ID ELSE r.STT_ID END         AS STT_ID',
            'CASE WHEN r.BASE_DT IS NULL THEN i.BASE_DT ELSE r.BASE_DT END      AS BASE_DT',
            'NVL(r.RENT_CNT,0) + NVL(i.RENT_CNT,0)                              AS RENT_CNT',
            'NVL(r.RETURN_CNT,0) + NVL(i.RETURN_CNT,0)                          AS RETURN_CNT'
        ).filter(col('STT_ID').isNotNull() | col('BASE_DT').isNotNull())

        return return_df



if __name__ == '__main__':
    rt_bicycle_rent = RtBicycleRent(app_name='dataframe_to_parameter')
    rt_bicycle_rent.main()