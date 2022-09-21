
import ast
from rest_framework.response import Response
from rest_framework import generics, status
from wrapper.serializers import (KafkaBrokerSerializer , KafkaConsumerSerializer, SplunkConsumerSunscribeSerializer)
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from wrapper.Kafka import Kafka
import logging


logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
logger = logging.getLogger(__name__)

class GetKafkaTopics(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, *args, **kwargs):
        payload  = {
            "Result" : "Success",
            "Data" : "",
            "Error_Message" : "None"
        }
        file_serializer = KafkaBrokerSerializer(data=request.data, context={'request': request})
        try:
            if file_serializer.is_valid():
                server = {}
                server.update({"bootstrap_servers" : file_serializer.data.get("bootstrap_servers")})
                logger.info(server)
                kafka_instance = Kafka(**server)
                data = kafka_instance.get_list_of_topic_and_partition()
                topic = ast.literal_eval(data)
                payload["Data"] = topic
                return Response(payload, status=status.HTTP_201_CREATED)
        except Exception as error:
            payload["Result"] = "Failed"
            payload["Error_Message"] = str(error)

            return Response(payload, status=status.HTTP_409_CONFLICT)
        return Response(file_serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class SplunkSubscribeAgent(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, *args, **kwargs):
        payload = {
            "Message": "",
            "Result": "Success",
            "Error_Message": []
        }
        file_serializer = SplunkConsumerSunscribeSerializer(data=request.data, context={'request': request})
        try:
            event_collector = dict()
            _topic_list = list()
            print (file_serializer)
            if file_serializer.is_valid():
                bootstrap_servers = file_serializer.data.get("bootstrap_servers")
                topics = file_serializer.data.get("topics")
                _topic_list.append(topics)
                partition = file_serializer.data.get("partition")
                subscribe = file_serializer.data.get("subscribe")
                event_collector.update({"bootstrap_servers": bootstrap_servers,
                                        "topics" : tuple(topics),
                                        "partition" :partition,
                                        "subscribe" : subscribe,
                                        "http_event_collector_key" : file_serializer.data.get("http_event_collector_key"),
                                        "http_event_collector_host": file_serializer.data.get("http_event_collector_host"),
                                        "index": file_serializer.data.get("index"),
                                        "sourcetype": file_serializer.data.get("sourcetype"),
                                        "source": file_serializer.data.get("source"),
                                        "host": file_serializer.data.get("host"),
                                        })
                kafka_instance = Kafka(topics,bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
                # if subscribe is True:
                #     kafka_instance.subscribe(topics,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
                kafka_instance.event_flush(**event_collector)
                payload["Message"]= "Thread Process has been started"
                return Response(payload, status=status.HTTP_201_CREATED)
            return Response(file_serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as error:
            payload["Result"] = "Failed"
            payload["Error_Message"].append(str(error))
            return Response(payload, status=status.HTTP_409_CONFLICT)