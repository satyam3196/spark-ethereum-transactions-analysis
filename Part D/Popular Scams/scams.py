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
            field = l.split(',')
            if len(field)!=15:
                return False
            int(field[11])
            str(field[6])
            float(field[7])
            return True
        except:
            return False
    
    def check_scam(l):
        try:
            field = l.split(',')
            if len(field)!=8:
                return False
            int(field[0])
            str(field[4])
            str(field[6])
            return True
        except:
            return False
    
    transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scam = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")

    
    tran = transaction.filter(check_transaction)
    sc = scam.filter(check_scam)
    
    sf = sc.map(lambda x: (x.split(',')[6], (x.split(',')[0],x.split(',')[4])))
    tran_map = tran.map(lambda x:  (x.split(',')[6], float(x.split(',')[7])))
    join = tran_map.join(sf)

    mapping = join.map(lambda x: ((x[1][1][0], x[1][1][1]),x[1][0]))
    popular_scam= mapping.reduceByKey(lambda a,b: a+b)
    popular_scam = popular_scam.map(lambda a: ((a[0][0],a[0][1]),float(a[1])))
    top10_scams = popular_scam.takeOrdered(15, key=lambda l: -1*l[1])
    print(popular_scam.take(10))
    
    sf1 = sc.map(lambda l: (l.split(',')[6], l.split(',')[4]))
    tran_map1 = tran.map(lambda x:  (x.split(',')[6], (time.strftime("%m/%Y",time.gmtime(int(x.split(',')[11]))),x.split(',')[7])))
    join1 = tran_map1.join(sf1)
    
    mapping1 = join1.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))
    ethert= mapping1.reduceByKey(lambda a,b: a+b)
    ethert = ethert.map(lambda a: ((a[0][0],a[0][1]),float(a[1])))
    print(ethert.take(10))

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)


    my_result_object = my_bucket_resource.Object(s3_bucket,'partd_scams_' + '/most_lucrative_scam_forms.txt')
    my_result_object.put(Body=json.dumps(top10_scams))               
    my_result_object = my_bucket_resource.Object(s3_bucket,'partd_scams_' + '/ether_received_over_time.txt')
    my_result_object.put(Body=json.dumps(ethert.take(100)))

    spark.stop()