from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json

#建立消息队列
def producer_demo(host,topic,value):
    producer = KafkaProducer(
        bootstrap_servers=host, 
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode() )
    # 发送消息
    future = producer.send(topic,value)
    print("生成数据:",value)
    try:
        future.get(timeout=10)  # 监控是否发送成功           
    except kafka_errors:        # 发送失败抛出kafka_errors
        traceback.format_exc()

if __name__ == "__main__":
    host=['localhost:9092']
    topic='jimmy_test'
    for i in range(100):
        data={"data":"list","num":str(i)}
        producer_demo(host,topic,data)
