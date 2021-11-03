import pandas as pd
import os
import sys
import datetime
import numpy as np
import traceback
from hdfs import InsecureClient
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import udf
from dependencies.spark import Spark
from dependencies.utils import *
from pyspark.ml.feature import CountVectorizer, StopWordsRemover

try:
    '''
    ES 및 spark와 연결
    '''
    config = ConfigParser()
    config.read('config.ini')
    spark = Spark(config, app_name='jhkim_job')
    spark_sess = spark.spark_session
    reader = spark.es_reader
    reader = reader.option("es.read.field.exclude", "host.ip, host.mac")
    spark_sess.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

except Exception as ex:
    print('spark_session 연결 실패 : ', ex)


def retrieve_data_from_es():
    '''
    ES에서 로드된 데이터를 dataframe으로 저장한 후 벡터화를 진행하여 전처리를 진행하기 위한 최종적인 데이터를 생성
    생성된 전처리 데이터 결과 값을 hdfs에 저장
    '''
    try:
        now = datetime.datetime.now()  # 마이그레이션을 위한 데이터 양 조절용
        date = now.strftime('%Y.%m.%d')
        df_syslog = reader.load("sym-log-syslog-{}".format(date))
        df_syslog.printSchema()
    except Exception as ex:
        traceback.print_exc()

    try:
        df_syslog = df_syslog.select(F.col('@timestamp').alias('timestamp'),
                                     F.col("host.hostname").alias('hostname'),
                                     F.col('host.id').alias('id'),
                                     F.col('log.file.path').alias('path'),
                                     F.col('message').alias('message'),
                                     F.col('pid').alias('pid'),
                                     F.col('program').alias('program'))

        # beats 관련 로그 필터링(추후 logstash에서 반영)
        df_syslog = df_syslog.filter(
            (F.col('program') != 'filebeat') & (F.col('program') != 'metricbeat'))

        df_syslog.withColumn('word', F.explode(F.split(F.col('message'), ' '))) \
            .groupBy('word') \
            .count() \
            .sort('count', ascending=False)

        df_syslog = df_syslog.withColumn('word_array', F.split(F.col('message'), ' '))

        # stopword remove
        remover = StopWordsRemover(inputCol="word_array", outputCol="word_array_f1")
        df_syslog = remover.transform(df_syslog)
        cv = CountVectorizer(inputCol='word_array_f1', outputCol='sparse_features',
                             vocabSize=500, minDF=3)
        model = cv.fit(df_syslog)
        result = model.transform(df_syslog).fillna(0)

    except Exception as ex:
        print('데이터 로드 실패 : ', ex)

if __name__ == '__main__':
    retrieve_data_from_es()
