import json
import pymysql
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
#https://kafka-python.readthedocs.io/en/master/usage.html

# CREATE TABLE `test_jimmy_kafka_data` (
#   `id` bigint NOT NULL AUTO_INCREMENT,
#   `topic` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
#   `partition_nb` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
#   `offset` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
#   `groupid` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
#   `req_data` longtext COLLATE utf8mb4_bin,
#   `cdate` datetime DEFAULT CURRENT_TIMESTAMP,
#   PRIMARY KEY (`id`)
# ) 

#消费消息
def consumer_demo(host,topic,groupid=None):
    consumer = KafkaConsumer(topic
        ,bootstrap_servers=host       #kafka服务host
        ,group_id=groupid             #消费者ID
        ,auto_offset_reset='earliest' #从上一次未消费的位置开始读
        ,enable_auto_commit=False     #消费完数据后手动commit，一条一条获取
        )
    print('消费服务已启动！')
    db=pymysql.connect(host='xxxx',port=3306,user='xx',passwd='xx',db='xx',charset='utf8')
    cur = db.cursor()
    for message in consumer:
        data=json.loads(message.value.decode())
        result=[topic,str(message.partition),str(message.offset),groupid,str(data)]
        print (" 消费消息：%s/%d/%d：value=%s" % (message.topic,message.partition,message.offset,data))
        sql="insert into test_jimmy_kafka_data(topic,partition_nb,offset,groupid,req_data) values(%s,%s,%s,%s,%s)"
        cur.execute(sql,result)
        db.commit()
        consumer.commit()
    db.close()
    cur.close()

if __name__ == "__main__":
    host=['ip1:9092','ip2:9092','ip3:9092']
    topic='jimmy_test'
    groupid='jimmy'      
    consumer_demo(host,topic,groupid)
