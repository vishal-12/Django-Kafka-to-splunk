# Generated by Django 4.0.3 on 2022-09-22 17:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('wrapper', '0005_alter_kafkaadminclientmodel_partition_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='kafkaadminclientmodel',
            name='client_id',
            field=models.CharField(default='client_id', max_length=1000),
        ),
    ]
