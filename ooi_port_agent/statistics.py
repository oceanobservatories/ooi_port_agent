import logging
from collections import deque

import os
from pika import BasicProperties, URLParameters
from pika.adapters import TwistedProtocolConnection
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.python import log
from twisted.internet import reactor


class ReconnectingPikaFactory(ReconnectingClientFactory):
    def __init__(self, parameters, handler):
        self.maxDelay = 30
        self.parameters = parameters
        self.handler = handler

    def buildProtocol(self, addr):
        self.resetDelay()
        proto = TwistedProtocolConnection(self.parameters)
        proto.ready.addCallback(self.handler.get_channel)
        return proto

    def clientConnectionLost(self, connector, reason):
        log.msg('Lost rabbitMQ connection.  Reason:', reason)
        self.handler.channel = None
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.msg('RabbitMQ connection failed. Reason:', reason)
        self.handler.channel = None
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)


class StatisticsPublisher(object):
    def __init__(self, exchange='amq.direct', routing_key='port_agent_stats'):
        self.exchange = exchange
        self.routing_key = routing_key
        self.channel = None

    @inlineCallbacks
    def get_channel(self, connection):
        log.msg("StatisticsPublisher: Connected to RabbitMQ")
        channel = yield connection.channel()
        yield channel.queue_declare(queue=self.routing_key, durable=True)
        yield channel.queue_bind(exchange=self.exchange, queue=self.routing_key, routing_key=self.routing_key)
        self.channel = channel

    @inlineCallbacks
    def publish(self, message):
        if self.channel:
            log.msg('Publishing statistics message to RabbitMQ', logLevel=logging.INFO)
            properties = BasicProperties(delivery_mode=2)
            yield self.channel.basic_publish(exchange=self.exchange,
                                             routing_key=self.routing_key,
                                             body=message,
                                             properties=properties)
            log.msg('published statistics message', logLevel=logging.DEBUG)
        else:
            log.msg('Unable to publish statistics message, no connection to RabbitMQ', logLevel=logging.WARN)

    def connect(self):
        url = os.environ.get('AMQP_URL', 'amqp://localhost')
        parameters = URLParameters(url)
        reactor.connectTCP(parameters.host, parameters.port, ReconnectingPikaFactory(parameters, self))
