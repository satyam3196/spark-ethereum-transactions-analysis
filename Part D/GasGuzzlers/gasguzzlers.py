import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contract = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    valid_transaction = transaction.filter(lambda line: len(line.split(',')) == 15 and line.split(',')[9].replace('.', '', 1).isdigit() and line.split(',')[11].replace('.', '', 1).isdigit())
    valid_contract = contract.filter(lambda line: len(line.split(',')) == 6)
    
    average_gas_price = valid_transaction.map(lambda line: (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), (float(line.split(',')[9]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .sortByKey(ascending=True) \
    .map(lambda x: (x[0], str(x[1][0] / x[1][1])))
    
        
    tran_gas = valid_transaction.map(lambda line: (line.split(',')[6], (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), float(line.split(',')[8]))))
    final_contract = valid_contract.map(lambda x: (x.split(',')[0], 1))
    tran_cont_join = tran_gas.join(final_contract)
    average_gas_used = tran_cont_join.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1]))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .map(lambda a: (a[0], str(a[1][0] / a[1][1]))) \
    .sortByKey(ascending=True)
   
  
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)


    resultp = my_bucket_resource.Object(s3_bucket,'partd_gg_' + '/average_gas_price.txt')
    resultp.put(Body=json.dumps(average_gas_price.take(100)))               
    resultu = my_bucket_resource.Object(s3_bucket,'partd_gg_' + date_time + '/average_gas_used.txt')
    resultu.put(Body=json.dumps(average_gas_used.take(100)))

    spark.stop()