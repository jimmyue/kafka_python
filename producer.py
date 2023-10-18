import random
import traceback
import json
from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors

#建立消息队列
def producer_demo(host,topic,value):
    producer = KafkaProducer(
        bootstrap_servers=host, 
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode() )
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
        data={"name":fake.name(),
              "gender":random.choice(["男","女"]),
              "age":fake.random_int(min=18,max=90),
              "address":fake.address(),
              "company":fake.company(),
              "job":fake.job(),
              "phone":fake.phone_number(),
              "id_card":fake.ssn(),
              "email":fake.email()}
        # 生产消息
        producer_demo(host,topic,data)
