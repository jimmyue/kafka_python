import json
import pymysql
from kafka import KafkaConsumer,TopicPartition
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
    consumer = KafkaConsumer(topic
        ,bootstrap_servers=host       #kafka服务host
        ,group_id=groupid             #消费者ID
        ,auto_offset_reset='earliest' #从上一次未消费的位置开始读
        ,enable_auto_commit=False     #消费完数据后手动commit，一条一条获取
        )
    print('kafka消费服务已启动！')
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    # total
    toff = consumer.end_offsets(partitions)
    toff = [(key.partition, toff[key]) for key in toff.keys()]
    toff.sort()
    print("{}所有offset: {}".format(topic,str(toff)))
    # current
    coff = [(x.partition, consumer.committed(x)) for x in partitions]
    coff.sort()
    print("{}当前offset: {}".format(topic,str(coff)))
    # cal sum and left
    toff_sum = sum([x[1] for x in toff])
    cur_sum = sum([x[1] for x in coff if x[1] is not None])
    left_sum = toff_sum - cur_sum
    print("{}剩余未消费: {}".format(topic,left_sum))
    #消费数据，插入数据库
    db=pymysql.connect(host='10.10.22.172',port=3306,user='lexususer',passwd='Lexus.1234#',db='ways_lexus',charset='utf8')
    cur = db.cursor()
    for message in consumer:
        data=json.loads(message.value.decode())
        result=[topic,str(message.partition),str(message.offset),groupid,str(data)]
        print ("\n消费消息：%s/%d/%d：value=%s" % (message.topic,message.partition,message.offset,data))
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
