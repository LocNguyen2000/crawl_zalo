#!/usr/bin/env python

# bin dir: C:\Program Files\RabbitMQ Server\rabbitmq_server-3.8.16\sbin
# see number of queues: rabbitmqctl.bat list_queues
# declare new queue: channel.queue_declare(queue='hello')
import json
import pika
import sys
import requests
from requests.exceptions import ProxyError, ConnectionError, Timeout

from bs4 import BeautifulSoup

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
def get_info(proxies, phone, domain):
    url = domain + str(phone)

    print('[Crawl] proxy:', proxies.get('http'))
    print('[Crawl] phone:', phone)
    page = requests.get(url, proxies=proxies, timeout=30)
    soup = BeautifulSoup(page.content, 'html.parser')
    username = soup.find("h3")

    print('[Crawl] get:', username.text)


def consume_queue(channel, queue):
    data = None

    channel.queue_declare(queue)
    for _method, _properties, _body in channel.consume(queue=queue):
        data = (json.loads(_body.decode()))
        channel.basic_ack(_method.delivery_tag)
        break
    channel.cancel()

    return data


def main():
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
            get_info(proxies=proxies, phone=phone_number, domain=DOMAIN)
            request_count += 1
            print('[Crawl] success')
        except (ProxyError, ConnectionError, Timeout) as e:
            print('[Error] proxy', proxies.get('http'), 'need to change')
            proxies = transform_proxy(consume_queue(proxy_channel, PROXY_QUEUE))
            request_error = True
            request_count = 0
        except AttributeError as e:
            print('[Error] phone not exist || proxy block')

        if request_count == max_request:
            proxies = transform_proxy(consume_queue(proxy_channel, PROXY_QUEUE))
            request_count = 0
            request_error = False


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
