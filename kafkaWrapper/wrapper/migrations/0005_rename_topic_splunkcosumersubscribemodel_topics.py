# Generated by Django 4.1.1 on 2022-09-21 19:43

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('wrapper', '0004_rename_bootstrap_server_kafkabrokermodel_bootstrap_servers'),
    ]

    operations = [
        migrations.RenameField(
            model_name='splunkcosumersubscribemodel',
            old_name='topic',
            new_name='topics',
        ),
    ]