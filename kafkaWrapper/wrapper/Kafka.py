from kafka import (KafkaConsumer,
                   TopicPartition,
                   KafkaProducer)
import ssl
from threading import Thread
import json
import time
import ast
import sys
from wrapper.splunk_http_event_collector import http_event_collector
from kafka.admin import KafkaAdminClient, NewTopic
from time import gmtime, strftime
from wrapper.logger import logging

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return wrapper

class KafkaAdmin:
    def __init__(self,bootstrap_servers,client_id="Sample"):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id=self.client_id)

    def create(self,topics,partition):
        """
            Create Topics
        """
        topic_list = []
        topic_list.append(NewTopic(name=topics, num_partitions=partition, replication_factor=partition))
        self.client.create_topics(new_topics=topic_list, validate_only=False)
        self.logger.info("Topics Created")

class KafkaService:
    """
        Kafka class will use offical kafka module to interact with the kafka broker
           for adding/updating and fetching teh data from Kafka Consumer

           bootstrap_servers : 'host[:port]' string (or list of 'host[:port]'
 |          strings) that the consumer should contact to bootstrap initial
 |          cluster metadata. This does not have to be the full node list.
 |          It just needs to have at least one broker that will respond to a
 |          Metadata API Request. Default port is 9092. If no servers are
 |          specified, will default to localhost:9092.

           client_id (str): a name for this client. This string is passed in
 |          each request to servers and can be used to identify specific
 |          server-side log entries that correspond to this client. Also
 |          submitted to GroupCoordinator for logging with respect to
 |          consumer group administration. Default: 'kafka-python-{version}'

    """

    def __init__(self, *topics, **configs):
        # logging.basicConfig(filename="Kafka-logs.log",
        #                     filemode='a',
        #                     level=logging.DEBUG,
        #                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d',
        #                     datefmt='%H:%M:%S',)

        self.logger = logging()
        self.logger.set_log_file("kafka.logs")
        self.topics = topics
        self.configs = configs
        if not ("auto_offset_reset" in self.configs):
            self.configs.update({"auto_offset_reset": "earliest"})

        if not ("enable_auto_commit" in self.configs):
            self.configs.update({"enable_auto_commit": True})

        if not ("consumer_timeout_ms" in self.configs):
            self.configs.update({"consumer_timeout_ms": 60})
        if hasattr(ssl, '_create_unverified_context'):
            ssl._create_unverified_context()

        if self.configs.get("producer") is True:
            svr = list()
            bootstrap_servers = str(self.configs.get("bootstrap_servers"))
            svr.append(bootstrap_servers)
            self.logger.info("Producer -{} ".format(svr))
            del self.configs["producer"]
            self.broker_producer = KafkaProducer(bootstrap_servers=svr)

        if len(topics) == 0:
            self.consumer = KafkaConsumer(*self.topics, **self.configs)
        else:
            self.logger.info("Consumer ")
            self.consumer = KafkaConsumer(*self.topics, **self.configs)
        self.logger.info("Done ")

    def change_topics(self, *topics, **configs):
        self.client = KafkaConsumer(*topics, **configs)

    @threaded
    def push(self, partition,data):
        self.broker_producer.send(topic=self.topics[0],
                      partition=int(partition),
                      value=json.dumps(data).encode('utf-8')
                      )
        self.logger.info("Pushed the logs")

    def subscribe(self, *topics, **config):
        """
        #assign topic and partition for logs
        :param topic:
        :param partition:
        :return:
        """
        if "partition" in config:
            partition = config["partition"]
            del config["partition"]
        else:
            self.logger.info("Please Provide Partition")
        self.change_topics(*topics, **config)
        self.client.subscribe(*topics)

        # TO DO : Add the partition
        # consumer.subscribe(*topics)
        # partition = TopicPartition('52.5_13.4', 0)
        # end_offset = consumer.end_offsets([partition])
        # consumer.seek(partition, list(end_offset.values())[0] - 1)

    @threaded
    def event_flush(self, **event_collector):
        """
        :param event_collector:
        :return:
        """
        self.logger.info("Func [event_flush] Started")
        payload = {}
        # if "subscribe" in event_collector:
        #     if event_collector["subscribe"] is True:
        #         self.subscribe(event_collector["topics"],**event_collector["bootstrap_servers"])
        if "http_event_collector_key" in event_collector:
            http_event_collector_key = event_collector.get("http_event_collector_key")

        if "http_event_collector_host" in event_collector:
            http_event_collector_host = event_collector.get("http_event_collector_host")

        # change rge value in constructor
        self.logger.info("Splunk Constructor Initiated")
        testevent = http_event_collector(http_event_collector_key, http_event_collector_host,logger=self.logger)
        hec_reachable = testevent.check_connectivity()
        if not hec_reachable:
            sys.exit(1)

        testevent.popNullFields = True
        # set logging to DEBUG for example
        #testevent.log.setLevel(logging.DEBUG)
        payload.update({
            "index": event_collector.get("index"),
            "sourcetype": event_collector.get("sourcetype"),
            "source": event_collector.get("source"),
            "host": event_collector.get("host")

        })
        self.logger.info("Https Event Collector Details [{}]".format(payload))
        for event in self.consumer:
            if isinstance(event, dict):
                self.logger.info("Getting data with the help of Kafka consumer - {}".format(event.value))
                data = ast.literal_eval(event.value.decode('utf8'))
                payload.update({"event": data})
                testevent.sendEvent(payload)
                self.logger.info("[Data - {} : pushed to Splunk]".format(payload))
                self.logger.info("Going to sleep for 5 mins")
                time.sleep(5)
            else:
                self.logger.info("Data is not Json.Serialized")
        self.logger.info("task.status Success")

    def unsubscribe(self):
        self.client.unsubscribe()

    def assigments(self):
        return self.client.assignment()

    def get_list_of_topic_and_partition(self):
        """
        :return:
        """
        payload = {"topicWithPartitions": {"data": {"topics": []}}}
        topics = self.consumer.topics()
        if len(topics) != 0:
            for tps in topics:
                partition = [prt for prt in self.consumer.partitions_for_topic(tps)]
                payload["topicWithPartitions"]["data"]["topics"].append({"topic": tps, "partition": partition})
        return json.dumps(payload)

# # #
##aa= KafkaService("datacenters",bootstrap_servers = "52.41.31.91:29092",producer = True)
##aa.push(0,{"data":"test"})

# aa.subscribe_topic("fluentd-poc",bootstrap_servers = "52.41.31.91:39092",partition= [0])
# aa.get_list_of_topic_and_partition()


# #admin = KafkaAdminClient(bootstrap_servers ="52.41.31.91:39092")
#
# #Create a topic
# #client.create_topics
#
# #kafka Client
# client= KafkaClient(bootstrap_servers ="52.41.31.91:39092",client_id = "fluentd-poc")
#
# #topic =  client.add_topic("Fake_App")
#
# ##
#
# aa = {"error" : "500 json error","id": 14,"info":"Customer","job_id": "9"}
# producer = KafkaProducer(bootstrap_servers=["52.41.31.91:39092"])
# ##
# #Send data
# producer.send(topic="fluentd-poc",
#               partition=0,
#               #key=b"test01",
#               value=json.dumps(aa).encode('utf-8')
#               )
#
#
# Kafka Consumer
# consumer = KafkaConsumer("fluentd-poc",
# #     bootstrap_servers="52.41.31.91:39092",
# #    # value_deserializer=lambda v: json.dumps(v).encode("utf-8"),
# #      auto_offset_reset='earliest'
#                          )
# for i in consumer:
#     print (i)
#
#
# #Link - https://kafka-python.readthedocs.io/en/master/apidoc/ClusterMetadata.html
#
#
# #Listner  - https://kafka-python.readthedocs.io/en/master/
