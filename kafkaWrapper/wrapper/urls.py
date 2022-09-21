from django.urls import include, re_path,path
from . import views

urlpatterns = [
    re_path('^list/', views.GetKafkaTopics.as_view(), name='GetKafkaTopics'),
    re_path('^subscribe/', views.SplunkSubscribeAgent.as_view(), name='SplunkSubscribeAgent'),
]