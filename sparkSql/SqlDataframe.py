from time import time

from pyspark.sql import SparkSession, SQLContext, Row
# from urllib.request import urlretrieve
# f = urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    data_file = "./kddcup.data_10_percent.gz"
    session = SparkSession.builder.appName("sql_test").master("local[*]").getOrCreate()
    sc = session.sparkContext
    sqlContext = SQLContext(sc)
    raw_data = sc.textFile(data_file).cache()

    csv_data = raw_data.map(lambda l: l.split(","))
    row_data = csv_data.map(lambda p: Row(
        duration=int(p[0]),
        protocol_type=p[1],
        service=p[2],
        flag=p[3],
        src_bytes=int(p[4]),
        dst_bytes=int(p[5])
    )
                            )

    # schema = StructType([StructField("duration", IntegerType(), True),
    #                      StructField("protocol_type", StringType(), True),
    #                      StructField("service", StringType(), True),
    #                      StructField("flag", StringType(), True),
    #                      StructField("src_bytes", IntegerType(), True),
    #                      StructField("dst_bytes", IntegerType(), True)])

    interactions_df = sqlContext.createDataFrame(row_data)
    interactions_df.createOrReplaceTempView("interactions")
    tcp_interactions = sqlContext.sql("""
        SELECT duration, dst_bytes FROM interactions WHERE protocol_type = 'tcp' AND duration > 1000 AND dst_bytes = 0
    """)
    tcp_interactions.show()

    tcp_interactions_out = tcp_interactions.rdd.map(lambda p: "Duration: {}, Dest. bytes: {}".format(p.duration, p.dst_bytes))
    for ti_out in tcp_interactions_out.collect():
        print(ti_out)

    interactions_df.printSchema()

    t0 = time()
    interactions_df.select("protocol_type", "duration", "dst_bytes").groupBy("protocol_type").count().show()
    tt = time() - t0

    print("Query performed in {} seconds".format(round(tt, 3)))
