import json
import pymysql
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
#https://kafka-python.readthedocs.io/en/master/usage.html

# CREATE TABLE `test_jimmy_kafka_data` (
#   `id` bigint NOT NULL AUTO_INCREMENT,
#   `topic` varchar(50) NOT NULL COMMENT '主题',
#   `partitions` varchar(50) DEFAULT NULL COMMENT '分区',
#   `offset` varchar(50) DEFAULT NULL COMMENT '偏移量',
#   `groupid` varchar(50) DEFAULT NULL COMMENT '消费者id',
#   `req_data` longtext COMMENT '消息数据',
#   `cdate` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '消费时间',
#   PRIMARY KEY (`id`)
# ) 

#消费消息
def consumer_demo(host,topic,groupid=None):
    consumer = KafkaConsumer(topic    #主题
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
        sql="insert into test_jimmy_kafka_data(topic,partitions,offset,groupid,req_data) values(%s,%s,%s,%s,%s)"
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
