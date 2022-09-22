
import ast
from rest_framework.response import Response
from rest_framework import generics, status
from wrapper.serializers import (KafkaBrokerSerializer ,
                                 KafkaConsumerSerializer,
                                 SplunkConsumerSunscribeSerializer,
                                 KafkaProducerSerializer,
                                 KafkaAdminClientSerializer)
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from wrapper.Kafka import (KafkaService,
                            KafkaAdmin)
import logging
from django.http import HttpResponse
import os
from pathlib import Path
import logging
from django.conf import settings as conf_settings

current_dir = Path(os.getcwd()).resolve()
# project_path = conf_settings.PROJECT_PATH


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(name)-12s %(levelname)-8s %(message)s'
        },
        'file': {
            'format': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'file',
            'filename': os.path.join(current_dir, "logs/application.log")
        }
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console', 'file']
        }
    }
})

logger = logging.getLogger(__name__)

class KafkaAdminClientView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, *args, **kwargs):
        payload  = {
            "Result" : "Success",
            "Error_Message" : "None"
        }
        file_serializer = KafkaAdminClientSerializer(data=request.data, context={'request': request})
        try:
            if file_serializer.is_valid():
                bootstrap_servers = file_serializer.data.get("bootstrap_servers")
                topics = file_serializer.data.get("topics")
                client_id = file_serializer.data.get("client_id")
                partition = file_serializer.data.get("partition")
                kafka_admin = KafkaAdmin(bootstrap_servers,client_id)
                kafka_admin.create(topics,partition)
                return Response(payload, status=status.HTTP_201_CREATED)
        except Exception as error:
            payload["Result"] = "Failed"
            payload["Error_Message"] = str(error)
            return Response(payload, status=status.HTTP_409_CONFLICT)
        return Response(file_serializer.errors, status=status.HTTP_400_BAD_REQUEST)

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
                kafka_instance = KafkaService(**server)
                data = kafka_instance.get_list_of_topic_and_partition()
                topic = ast.literal_eval(data)
                payload["Data"] = topic
                return Response(payload, status=status.HTTP_201_CREATED)
        except Exception as error:
            payload["Result"] = "Failed"
            payload["Error_Message"] = str(error)

            return Response(payload, status=status.HTTP_409_CONFLICT)
        return Response(file_serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class GetProducerView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, *args, **kwargs):
        payload  = {
            "Result" : "Success",
            "Error_Message" : "None"
        }
        file_serializer = KafkaProducerSerializer(data=request.data, context={'request': request})
        try:
            if file_serializer.is_valid():
                server = {}
                server.update({"bootstrap_servers" : file_serializer.data.get("bootstrap_servers"),
                              "producer" : True
                                })
                partition = file_serializer.data.get("partition")
                data = file_serializer.data.get("data")
                kafka_instance = KafkaService(file_serializer.data.get("topics"), **server)
                kafka_instance.push(partition,data)
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
                kafka_instance = KafkaService(topics,bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
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