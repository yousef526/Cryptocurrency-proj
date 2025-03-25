from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,explode, col, to_timestamp
from pyspark.sql.types import TimestampNTZType, StringType ,StructType, ArrayType, DoubleType, StructField
from functools import partial


batch_num = 0

def processData():
    spark = SparkSession.builder\
        .appName("CryptoProj") \
        .config("spark.jars", "/opt/airflow/dags/CryptoScripts/postgresql-42.7.5.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    num = 0
    with open("/opt/airflow/dags/CryptoScripts/partNum.txt","r") as f:
        num = int(f.readline())

    schema = StructType([
    StructField("asset_id_base", StringType(), True),
    StructField("rates", ArrayType(
        StructType([
            StructField("time", StringType(), True), 
            StructField("asset_id_quote", StringType(), True),
            StructField("rate", DoubleType(), True)])
            ), True)
    ])
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "Topic_1") \
    .option("startingOffsets", "latest") \
    .load()
    
    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df = df_parsed.select(from_json(col("json_str"), schema).alias("data"))

    # Explode the rates array if needed
    exploded_df = parsed_df.select(
        col("data.asset_id_base"),
        col("data.rates")
    ).withColumn("rate", col("rates")) \
    .selectExpr("asset_id_base", "explode(rate) as rate_data") \
    .select(
        col("asset_id_base").alias("Base_currency"),
        col("rate_data.time").cast(TimestampNTZType()).alias("time"),
        col("rate_data.asset_id_quote").alias("Other_currency"),
        col("rate_data.rate").alias("Rate_change")
    )


    # Show the final DataFrame
    """ df_final.show(n=2,truncate=False)
    df_final.printSchema() """

    #my_batch_func = partial(write_to_postgres, df_parsed, num)
    global batch_num
    batch_num = num
    print("Stuck here")
    query = exploded_df.writeStream \
    .foreachBatch(write_to_postgres,) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/airflow/dags/CryptoScripts/Spark_chkP/topic_1")\
    .start()

    print("Stuck here222")
    query.awaitTermination(30)
    query.stop()

    with open("/opt/airflow/dags/CryptoScripts/partNum.txt","w") as f:
        num+=1
        f.write(str(num))

def write_to_postgres(batch_df,batch_id):
    global batch_num
    batch_id = batch_num
    print("Stuck here2")

    if batch_df.isEmpty():
        print(f"⚠️ No data in batch {batch_id}")
    else:
        print(f"✅ Batch {batch_id} has data: {batch_df.count()} rows")

    json_output_path = f"/opt/airflow/dags/CryptoScripts/GeneratedData/{batch_id}.json"
    batch_df.write \
        .mode("overwrite") \
        .json(json_output_path)
    

    batch_df.printSchema()
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
        .option("dbtable", "public.CryptoProj") \
        .option("user", "postgres") \
        .option("password", "123456789") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append")\
        .save()
    print("Data added to Db succesully")
    
    