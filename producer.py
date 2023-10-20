import random
import json
import traceback
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import kafka_errors
#https://kafka-python.readthedocs.io/en/master/usage.html

#建立消息队列
def producer_demo(host,topic,value):
    producer = KafkaProducer(
        bootstrap_servers=host, 
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode() 
        )
    # 发送消息
    future = producer.send(topic,value)
    print("\nTOPIC:",topic," 生产消息:",value)
    try:
        future.get(timeout=10)  # 监控是否发送成功           
    except kafka_errors:        # 发送失败抛出kafka_errors
        traceback.format_exc()
    producer.close()

if __name__ == "__main__":
    host=['localhost:9092']
    topic='jimmy_test'
    # faker生成测试数据
    fake = Faker(locale='zh_CN')
    for i in range(10):
        data={
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
        producer_demo(host,topic,data)
