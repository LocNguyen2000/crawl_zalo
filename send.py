#!/usr/bin/env python

import pika
import json
import os

PROXY_QUEUE = 'proxy'
PHONE_QUEUE = 'phone'

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

dirname = os.path.dirname(__file__)
# REMEMBER: fix path
proxy_file = os.path.join(dirname, './proxy_list_test.json')
phone_file = os.path.join(dirname, './phone_number.json')


def send_to_queue(_channel, _queue, _file):
    channel.queue_declare(_queue)

    for line in open(_file, encoding='utf-8', mode='r'):
        message = json.loads(line)
        _channel.basic_publish(
            # REMEMBER: exchange = Test
            exchange='',
            routing_key=_queue,
            body=json.dumps(message)
        )
    print(" [x] Sent data to RabbitMQ")


send_to_queue(_channel=channel, _queue=PHONE_QUEUE, _file=phone_file)
send_to_queue(_channel=channel, _queue=PROXY_QUEUE, _file=proxy_file)

connection.close()
