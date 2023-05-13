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
    
    def check_transaction(l):
        try:
            fields = l.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            return True
        except:
            return False

    def check_contract(l):
        try:
            fields = l.split(',')
            if len(fields)!=6:
                return False
            else:
                return True
        except:
            return False

    transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contract = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")


    tran = transaction.filter(check_transaction)
    cont = contract.filter(check_contract)

    flag_t = tran.map(lambda x: (x.split(',')[6], int(x.split(',')[7])))
    flag_c = cont.map(lambda x: (x.split(',')[0],1))
    tran_reducer = flag_t.reduceByKey(lambda x, y: x + y)
    tran_cont_join = tran_reducer.join(flag_c)
    value = tran_cont_join.map(lambda x: (x[0], x[1][0]))
    topservices = value.takeOrdered(10, key=lambda l: -1*l[1])

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    result = my_bucket_resource.Object(s3_bucket,'partb_' + '/top_10_services.txt')
    result.put(Body=json.dumps(topservices))
    
    spark.stop()