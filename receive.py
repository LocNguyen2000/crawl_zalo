#!/usr/bin/env python

# bin dir: C:\Program Files\RabbitMQ Server\rabbitmq_server-3.8.16\sbin
# see number of queues: rabbitmqctl.bat list_queues
# declare new queue: channel.queue_declare(queue='hello')
import json
import pika
import sys
import threading
import time
from bs4 import BeautifulSoup
import requests
from requests.exceptions import ProxyError, ConnectionError, Timeout

PROXY_QUEUE = 'proxy'
PHONE_QUEUE = 'phone'
DOMAIN = 'https://zalo.me/'


def transform_proxy(message):
    proxy = message.get('IP_Address') + ":" + message.get('Port')
    if message.get('Protocol') == 'SOCKS4':
        proxy = 'socks4://' + str(proxy)
    else:
        proxy = str(proxy)
    return {
        'http': proxy,
        'https': proxy
    }


# - lấy proxy
# => sđt > gặp proxy hỏng
# => chuyển proxy (giữ sđt) gặp lỗi ko có html attribute
# => chuyển sđt

# proxy, connection, timeout,  html
def get_info(thread, proxies, phone, domain):
    url = domain + str(phone)

    print('[Crawl] {} proxy:'.format(thread), proxies.get('http'))
    print('[Crawl] {} phone:'.format(thread), phone)
    page = requests.get(url, proxies=proxies, timeout=30)
    soup = BeautifulSoup(page.content, 'html.parser')
    username = soup.find("h3")

    print('[Crawl] {} get:'.format(thread), username.text)


def consume_queue(channel, queue):
    data = None

    channel.queue_declare(queue)
    for _method, _properties, _body in channel.consume(queue=queue):
        data = (json.loads(_body.decode()))
        channel.basic_ack(_method.delivery_tag)
        break
    channel.cancel()

    return data


def main(thread_index):
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)

    proxy_channel = connection.channel()
    phone_channel = connection.channel()

    done = True
    max_request = 5
    request_count = 0
    request_error = False

    proxies = transform_proxy(consume_queue(proxy_channel, PROXY_QUEUE))
    phone_number = None

    while done:
        if not request_error:
            phone_data = consume_queue(phone_channel, PHONE_QUEUE)
            phone_number = phone_data.get('phone_number')
        elif request_error:
            request_error = False

        try:
            get_info(thread=thread_index, proxies=proxies, phone=phone_number, domain=DOMAIN)
            request_count += 1
            print('[Crawl] {} success'.format(thread_index))
        except (ProxyError, ConnectionError, Timeout) as e:
            print('[Error] {} proxy'.format(thread_index), proxies.get('http'), 'need to change')
            proxies = transform_proxy(consume_queue(proxy_channel, PROXY_QUEUE))
            request_error = True
            request_count = 0
        except AttributeError as e:
            print('[Error] {} phone not exist || proxy block'.format(thread_index))

        if request_count == max_request:
            proxies = transform_proxy(consume_queue(proxy_channel, PROXY_QUEUE))
            request_count = 0
            request_error = False


if __name__ == '__main__':
    try:
        threads = []
        num_thread = 2
        for index in range(num_thread):
            thread = threading.Thread(target=main, args=(index,))
            thread.start()
            threads.append(thread)
        for i in range(num_thread):
            threads[i].join()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
