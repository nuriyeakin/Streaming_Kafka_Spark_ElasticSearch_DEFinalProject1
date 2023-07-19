# Streaming_Kafka_Spark_ElasticSearch_DEFinalProject1

### 0. Data and DataOps 

- This dataset was collected from 255 sensor time series located in 51 rooms on 4 floors of the Sutardja Dai Hall (SDH) at UC Berkeley. - - It can be used to investigate patterns in the physical properties of a room in a building. 
- It can also be used for experiments related to Internet of Things (IoT), sensor fusion network or time series tasks. 
- This dataset is suitable for both supervised (classification and regression) and unsupervised learning (clustering) tasks.

Each room contains 5 types of measurements:

```
- CO2 concentration,
- humidity: room air humidity,
- temperature: room temperature,
- light: brightness
- PIR: motion sensor data
```
- Data were collected over a one-week period from Friday, August 23, 2013 to Saturday, August 31, 2013. 
- The motion sensor (PIR) generates a value every 10 seconds. The remaining sensors generate a value every 5 seconds. 
- Each file contains the timestamp and sensor value inside.

- A passive infrared sensor (PIR sensor) is an electronic sensor that measures infrared (IR) light emitted by objects in the field of view and measures the occupancy in a room. 
- About 6% of the PIR data is non-zero and indicates the room occupancy. The remaining 94% of the PIR data is zero, indicating an empty room.

### 1.Task

- 01. Convert the dataset to the figure below and save it to the local disk.

``` 
            event_ts_min 	ts_min_bignt room 	co2           light 	    temp 	    humidity 	pir
0 	2013-08-24 02:04:00 	1377299040 	656A 	578.500000 	  176.500000 	24.370001 	49.900002 	28.500000 
1 	2013-08-24 02:04:00 	1377299040 	746 	633.000000 	  29.000000 	23.059999 	52.840000 	21.000000
2 	2013-08-24 02:05:00 	1377299100 	413 	494.727273 	  96.555556 	23.927778 	45.330001 	0.000000
3 	2013-08-24 02:05:00 	1377299100 	421 	362.900000 	  194.700000 	22.837000 	52.877999 	0.000000
``` 

- 02. Produce the dataset from the local disk into a topic named Kafka `office-input` with the data-generator.

- 03. Consume the Kafka `office-input` topic with Spark Streaming. 

- Structuring the data (dressing the diagram above) write it to Elasticsearch so that each room name is an index. 

- It will be enough to do it for 3-4 rooms

- Visualize the index of each room with Kibana.

- 04. Visualize the index of each room with Kibana.



### 2. Start Jupyter Lab
```
[train@trainvm ]$ cd Final_project1
```
```
[train@trainvm Final_project1]$ source ~/venvspark/bin/activate
```
```
(venvspark) [train@trainvm Final_project1]$ jupyter lab
```

### 3. Cleaning the Data
```
Continue from data_cleaning.ipnb
```

### 4. Stop kafka local
```
sudo systemctl stop kafka

sudo systemctl stop zookeeper
```

### 5. Kafka Multinode Docker Zookeepeerless

```
[train@trainvm]$ sudo systemctl start docker

[train@trainvm]$ cd dataops7

[train@trainvm dataops7]$ cd kafka

[train@trainvm kafka]$ cd zookeeperless_kafka/

[train@trainvm zookeeperless_kafka]$ for i in {1..3}; do mkdir -p data/kafka${i}; done

[train@trainvm zookeeperless_kafka]$ docker-compose up --build -d
```

* Check cluster
```
docker-compose ps

```
### 6. Elastic Search
```
[train@trainvm elasticsearch]$ docker-compose up -d
[train@trainvm elasticsearch]$ docker-compose ps
```

### 7. Data-Generator file

```
[train@trainvm]$ cd data-generator

[train@trainvm data-generator]$ python3 -m virtualenv datagen

[train@trainvm data-generator]$ source datagen/bin/activate

(datagen) [train@trainvm data-generator]$ pip install -r requirements.txt #(if necessary)

[train@trainvm zookeeperless_kafka]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-input --replication-factor 1 --partitions 3

(datagen) [train@trainvm data-generator]$ python dataframe_to_kafka.py --input /home/train/datasets/final/dffinal/part-00000-4468d658-8987-4289-bad6-7c08db248523-c000.csv -t office-input

```
* Run sent_elasticsearch.py
* Check localhost:5601














