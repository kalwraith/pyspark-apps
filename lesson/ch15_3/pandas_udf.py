from common.ch13_1.base_stream_app import BaseStreamApp
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime
import pandas as pd


class PandasUdf(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name

    def _main(self):
        pandas_udf_get_birth = pandas_udf(PandasUdf.get_birth2, IntegerType())
        spark = self.get_spark_session(SparkSession)
        df = spark.createDataFrame([('kim','seoul',30),('park','busan',21)],'NAME STRING, ADDRESS STRING, AGE INT')
        df = df.withColumn(
            'BIRTH_YEAR_STT_METHOD',
            PandasUdf.get_birth('AGE')
        ).withColumn(
            'BIRTH_YEAR_DECORATOR',
            pandas_udf_get_birth('AGE')
        )
        df.show()

    # Decorator 적용 방식
    @staticmethod
    @pandas_udf(returnType=IntegerType())
    def get_birth(value: pd.Series):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year

    # 함수 적용 방식
    @staticmethod
    def get_birth2(value: pd.Series):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year
