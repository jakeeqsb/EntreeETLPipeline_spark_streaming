import threading
from subprocess import call

from kafka import KafkaProducer,KafkaConsumer
import glob
import Utilities
import time
import json
import concurrent.futures



class SessionProducer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


    def close(self):
        self.producer.close()

    def run(self):
        for session_file in glob.glob('./entree/session/session.*'):
            session = Utilities.getSessionData(session_file)
            print(session)
            msg = json.dumps(session).encode('utf-8')
            self.producer.send('session_data', msg)
            time.sleep(1)


def delete_kafka_topic(topic_name):
    call(["/usr/local/bin/kafka-topics", "--zookeeper", "zookeeper-1:2181", "--delete", "--topic", topic_name])

#delete_kafka_topic('session_data')

if __name__ == '__main__':

    sessionProducer = SessionProducer()
    sessionProducer.daemon = True
    sessionProducer.start()

    while(True):
        time.sleep(5)

