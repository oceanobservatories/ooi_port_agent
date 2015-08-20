from twisted.internet import reactor
from twisted.internet.defer import succeed
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface import implements

__author__ = 'petercable'

class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


def get(url):
    agent = Agent(reactor)
    return agent.request('GET', url)

def put(url, data):
    agent = Agent(reactor)
    producer = StringProducer(data)
    return agent.request('PUT', url, Headers(), producer)