from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import Utilities
from EntreeKafkaProd import SessionProducer
import json
import time

def printResult(rdd):
    rddlist = rdd.sortBy(lambda x:x[1],ascending=False).collect()
    rddlist = rddlist[:5]

    for r in rddlist:
        print("{0} visited by {1} users".format(r[0],r[1]))
    print("\n ... NEXT BATCH ... \n")

def run_pipe():

    # Create contexts
    sc = SparkContext(appName='EntreeDatapipeLine')
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 2)

    kvs = KafkaUtils.createDirectStream(ssc, ['session_data'],{"metadata.broker.list": 'localhost:9092'})

    session = kvs.flatMap(lambda x:json.loads(x[1]))

    chicagoRestMapTable = Utilities.mapRestIDName('./entree/data/chicago.txt')

    navigations = session.flatMap(lambda x: [ (chicagoRestMapTable[int(xi[:-1])],xi[-1]) for xi in x['navigations']])

    navigationsCount = navigations.map(lambda x:x[0]).countByValue()

    navigationsCount.foreachRDD(
        lambda x: printResult(x)
    )

    ssc.start()
    ssc.awaitTermination()

if '__main__' == __name__:

    run_pipe()

