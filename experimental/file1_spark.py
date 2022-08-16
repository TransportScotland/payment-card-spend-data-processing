from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.options(header='True', delimiter= ',') \
    .csv('data/file1_pa_1e4.csv')
df.printSchema()

# df.write.format('parquet').('file_table')

spark.sql("SELECT * FROM df LIMIT 10;")

# time_distinct = df.select('time_frame'
#     ).distinct(
#     )
# td_rdd = time_distinct.rdd.map(lambda el: str(el) + 'asd')
# # print(time_distinct.top(5))
# time_distinct = td_rdd.toDF(time_distinct.schema)
# time_distinct.show()


df.show(5)