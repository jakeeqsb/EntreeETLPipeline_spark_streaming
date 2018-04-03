# EntreeETLPipeline_spark_streaming

![screen shot 2018-04-03 at 2 07 46 am](https://user-images.githubusercontent.com/10664813/38240218-dc337d50-36e3-11e8-8d38-f7d74872f594.png)

## To run the program 
### 1. Run the zookeeper
#### zkServer start 
### 2. Run the kafka 
#### kafka-server-start <... server.properties>
### 3. Run the producer script 
#### python3 EntreeKafkaProd.py
### 4. Run the pipeline script
#### spark-submit --jars <jars...> EntreePipeline.py 

