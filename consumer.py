from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json

#消费消息
def consumer_demo(host,topic,groupid=None):
    consumer = KafkaConsumer(topic,group_id=groupid,bootstrap_servers=host)
    print('消费服务已启动！')
    for message in consumer:
        data=json.loads(message.value.decode())
        print('消费消息：',data)

if __name__ == "__main__":
    host=['localhost:9092']
    topic='jimmy_test'
    groupid='jimmy'      #消费者ID
    consumer_demo(host,topic,groupid)
