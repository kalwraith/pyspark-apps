from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('read_csv_write_parquet') \
        .getOrCreate()

path = 'hdfs:///home/spark/sample/csv/customers.csv'
schema = f'''Index LONG,
        "Customer Id" STRING,
        "First Name" STRING,
        "Last Name" STRING,
        Company STRING,
        City STRING,
        Country STRING,
        "Phone 1" STRING,
        "Phone 2" STRING,
        Email STRING,
        "Subscription Date" DATE,
        Website STRING'''
csv_df = spark.read \
          .option("header", True) \
          .schema(schema) \
          .csv(path)

csv_df.persist()
print(csv_df.count())

csv_df.write \
      .format('parquet') \
      .save('hdfs:///home/spark/sample/parquet/customers.parq')

print('parqet 파일 저장 완료 ')
