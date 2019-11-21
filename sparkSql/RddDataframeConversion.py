# -*- coding:utf-8 -*-
import functools
import sys
import os
import time

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_path)
# sys.path.insert(0, "D:\WorkSpaces\python3\python-spark-tutorial")
# print(sys.path)
from pyspark.sql import SparkSession
from python_spark_tutorial.commons.Utils import Utils


# for root, dirs, files in os.walk(base_path):
#     print('root_dir:', root)  # 当前目录路径
#     print('sub_dirs:', dirs)  # 当前路径下所有子目录
#     print('files:', files)  # 当前路径下所有非目录子文件


def time_me(info="used"):
    """
    cost time count
    Instructions:
    >>> @time_me()
        def test():
            return 1
    >>> test()
    main used 0.0240771 second

    >>> @time_me("watch")
        def test():
            return 1
    >>> test()
    main watch 0.0240771 second
    """

    def _time_me(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            start = time.perf_counter()
            fn(*args, **kwargs)
            print("%s %s %s" % (fn.__name__, info, time.perf_counter() - start), "second")
        return _wrapper
    return _time_me


def mapResponseRdd(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    double1 = None if not splits[6] else float(splits[6])
    double2 = None if not splits[14] else float(splits[14])
    return splits[2], double1, splits[9], double2


def getColNames(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return [splits[2], splits[6], splits[9], splits[14]]


@time_me()
def main():
    session = SparkSession.builder.appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    sc = session.sparkContext

    lines = sc.textFile("../in/2016-stack-overflow-survey-responses.csv")

    responseRDD = lines \
        .filter(lambda line: not Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(mapResponseRdd)

    colNames = lines \
        .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(getColNames)

    responseDataFrame = responseRDD.toDF(colNames.collect()[0])
    responseDataFrame.groupBy('country').count().show(20)
    print("=== Print out schema ===")
    responseDataFrame.printSchema()

    print("=== Print 20 records of responses table ===")
    responseDataFrame.show(20)

    for response in responseDataFrame.rdd.take(10):
        print(response)


if __name__ == "__main__":
    main()
