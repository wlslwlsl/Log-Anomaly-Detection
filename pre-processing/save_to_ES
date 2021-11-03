def write_data_to_es():
    df = update_preprocessing_data()
    try:
        # df 출력 스키마대로 뽑기
        df = df[['timestamp', 'hostname', 'program', 'pid', 'path', 'message', 'score', 'is_anomaly']]
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # df['timestamp'] = datetime.datetime.strptime(df['timestamp'], '%Y-%m-%d %H:%M:%S')

    except Exception as ex:
        print('출력 스키마 형태 구 실패 : ', ex)

    # spark dataframe 변경성
    try:
        spark_df = spark_sess.createDataFrame(data=df)
    except Exception as ex:
        print('spark dataframe 변경 실패 : ', ex)

    try:
        # schema 형식 맞추기
        spark_df = spark_df.withColumn('score',spark_df['score'].cast("float").alias('score'))
        spark_df = spark_df.withColumn('pid', spark_df['pid'].cast("float").alias('pid'))
        spark_df = spark_df.withColumn("is_anomaly", when(F.col("is_anomaly") == -1, 'True').otherwise(F.col('is_anomaly')))
        spark_df = spark_df.withColumn("is_anomaly", when(F.col("is_anomaly") == 1, 'False').otherwise(F.col('is_anomaly')))

        spark_df = spark_df.withColumn("is_anomaly", F.col("is_anomaly").cast('boolean'))

        # host를 위한 처리
        final_df = spark_df.withColumnRenamed('hostname', 'host.hostname') \
            .withColumn('pid', F.when(F.isnan(F.col('pid')), None).otherwise(F.col('pid')))
        
        #final_df.printschema()
        print('shcema change success')

    except Exception as ex:
        print('spark schema 형식 맞춤 실패 : ', ex)

    try:
        # es 데이터 삽입
        now = datetime.datetime.now()
        date = now.strftime('%Y.%m.%d')
        spark.save_to_es(final_df, 'sym-anomaly-log-{}'.format(date))
        print('data save')
    except Exception as ex:
        print('ES 데이터 적재 실패 : ', ex)

if __name__ == '__main__':
    write_data_to_es()
