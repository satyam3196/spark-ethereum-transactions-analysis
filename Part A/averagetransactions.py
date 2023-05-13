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
    
    def check_line(l):
        if len(l.split(',')) == 15:
            if l.split(',')[7].replace(".", "", 1).isdigit() and l.split(',')[11].isdigit():
                return True
        return False

    t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    lines = t.filter(check_line)
    
    date_val = lines.map(lambda line: (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), (float(line.split(',')[7]), 1)))

    reducer = date_val.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_transactions = reducer.map(lambda a: (a[0], str(a[1][0]/a[1][1])))
    average_transactions = average_transactions.map(lambda op: ','.join(str(tr) for tr in op))

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    result = my_bucket_resource.Object(s3_bucket,'part1b_' + '/average_transactions.txt')
    result.put(Body=json.dumps(average_transactions.take(100)))
    
    spark.stop()