import random
import json
import traceback
import pymysql
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import kafka_errors
#https://kafka-python.readthedocs.io/en/master/usage.html

# CREATE TABLE `test_jimmy_kafka_data` (
#   `id` bigint NOT NULL AUTO_INCREMENT,
#   `datatype` varchar(1) NOT NULL COMMENT '1为生产，2为消费',
#   `topic` varchar(50) NOT NULL COMMENT '主题',
#   `partitions` varchar(50) DEFAULT NULL COMMENT '分区',
#   `offset` varchar(50) DEFAULT NULL COMMENT '偏移量',
#   `groupid` varchar(50) DEFAULT NULL COMMENT '消费者id',
#   `req_key` varchar(50) DEFAULT NULL COMMENT '消费key',
#   `req_value` longtext COMMENT '消息value',
#   `cdate` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '消费时间',
#   PRIMARY KEY (`id`)
# ) 

#建立消息队列
def producer_demo(host,topic,key,value):
    producer = KafkaProducer(
        bootstrap_servers=host, 
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode() 
        )
    # 发送消息
    future = producer.send(topic,key=key,value=value)
    print("\n生产消息： topic=",topic," key=",key," value=",value)
    try:
        record_metadata=future.get(timeout=10)  # 监控是否发送成功           
    except kafka_errors:        # 发送失败抛出kafka_errors
        traceback.format_exc()
    producer.close()
    return record_metadata.partition,record_metadata.offset

if __name__ == "__main__":
    host=['IP1:9092','IP2:9092','IP3:9092'] #修改kafka服务地址
    topic='jimmy_test'                                              #修改需要生产消费的topic
    key='updateCustomer'                                            #修改生产消费的key
    db=pymysql.connect(host='XXXX',port=3306,user='XX',passwd='XX',db='XX',charset='utf8')
    cur = db.cursor()
    # faker生成测试数据
    fake = Faker(locale='zh_CN')
    for i in range(5):
        value={
              "name":fake.name(),
              "gender":random.choice(["男","女"]),
              "age":fake.random_int(min=18,max=90),
              "address":fake.address(),
              "company":fake.company(),
              "job":fake.job(),
              "phone":fake.phone_number(),
              "dealer":random.choice(["L0201","L0M33","L0R35","L0X55","L0B17","L0A11","L0U11","L0R30","L0R32","L0X54",None,""]),
              "id_card":fake.ssn(),
              "email":fake.email()
              }
        # 生产消息
        res=producer_demo(host,topic,key,value)
        result=['1',topic,res[0],res[1],key,str(value)]
        sql="insert into test_jimmy_kafka_data(datatype,topic,partitions,offset,req_key,req_value) values(%s,%s,%s,%s,%s,%s)"
        cur.execute(sql,result)
        db.commit()
    db.close()
    cur.close()
