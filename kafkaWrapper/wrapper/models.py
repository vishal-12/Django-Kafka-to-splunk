from django.db import models
from rest_framework.response import Response

class KafkaBrokerModel(models.Model):

    class Meta:
        db_table = 'tbl_broker'
        verbose_name = "Broker Data"
        verbose_name_plural = "Broker Access"
        ordering = ("-created_at",)

    id = models.AutoField(primary_key=True)
    bootstrap_servers = models.CharField(max_length=1000, default="Kafka Topic Data")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "{0}".format(self.id)

class KafkaConsumerModel(models.Model):

    class Meta:
        db_table = 'tbl_consumer'
        verbose_name = "Consumer Data"
        verbose_name_plural = "Consumer Access"
        ordering = ("-created_at",)

    id = models.AutoField(primary_key=True)
    data = models.CharField(max_length=1000, default="Kafka Topic Data")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "{0}".format(self.id)

class SplunkCosumerSubscribeModel(models.Model):

    class Meta:
        db_table = 'tbl_splunk_subscribe'
        verbose_name = "Splunk Data"
        verbose_name_plural = "Splunk Logs"
        ordering = ("-created_at",)

    id = models.AutoField(primary_key=True)
    bootstrap_servers = models.CharField(max_length=1000, default="Kafka Bootstrap Server Data")
    topics = models.CharField(max_length=1000, default="Kafka Topic Data")
    partition =  models.CharField(max_length=1000, default="Kafka Partition Data")
    subscribe = models.BooleanField(default=True)
    http_event_collector_key = models.CharField(max_length=1000, default="http_event_collector_key")
    http_event_collector_host = models.CharField(max_length=1000, default="http_event_collector_host")
    index = models.CharField(max_length=1000, default="Splunk index")
    sourcetype = models.CharField(max_length=1000, default="Sourcetype Splunk")
    source = models.CharField(max_length=1000, default="Source Splunk")
    host = models.CharField(max_length=1000, default="Splunk Host")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "{0}".format(self.id)