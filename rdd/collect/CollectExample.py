from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf().setAppName("collect").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    inputWords = ["spark-1", "hadoop-2", "spark-3", "hive-4", "pig-5", "cassandra-6", "hadoop-7"]

    wordRdd = sc.parallelize(inputWords)
    print(type(wordRdd))

    words = wordRdd.collect()

    for word in words:
        print(word)
