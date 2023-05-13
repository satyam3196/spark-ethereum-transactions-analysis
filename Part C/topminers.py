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
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  
    
    def check_block(l):
        try:
            fields = l.split(',')
            if len(fields) == 19 and fields[1] != 'hash':
                return True
            else:
                return False
        except:
            return False

    def feature_block(l):
        try:
            field = l.split(',')
            miner = str(field[9])
            size = int(field[12])
            return (miner, size)
        except:
            return (0, 1)
        
    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    valid_block=blocks.filter(check_block)
    flag_block = valid_block.map(feature_block)
    block_reducer = flag_block.reduceByKey(lambda a, b: a + b)
    top_miners = block_reducer.takeOrdered(10, key=lambda l: -l[1])
    
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    result = my_bucket_resource.Object(s3_bucket,'partc_' + '/top_10_miners.txt')
    result.put(Body=json.dumps(top_miners))
    
    spark.stop()