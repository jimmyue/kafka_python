import traceback
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors

#消费消息
def consumer_demo(host,topic,groupid=None):
    consumer = KafkaConsumer(topic,group_id=groupid,bootstrap_servers=host,auto_offset_reset='earliest')
    print('消费服务已启动！')
    for message in consumer:
        data=json.loads(message.value.decode())
        print("\nTOPIC:",topic,' 消费消息：',data)

if __name__ == "__main__":
    host=['localhost:9092']
    topic='jimmy_test'
    groupid='jimmy'      #消费者ID
    consumer_demo(host,topic,groupid)
