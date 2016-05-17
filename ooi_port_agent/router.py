import json
import logging
import time
from collections import Counter, defaultdict

from twisted.internet import task
from twisted.python import log

from common import EndpointType
from common import Format
from common import NEWLINE
from common import PacketType
from common import ROUTER_STATS_INTERVAL
from common import RouterStat


#################################################################################
# Port Agent Router
#################################################################################


class Router(object):
    """
    Route data to a group of endpoints based on endpoint type
    """

    def __init__(self, agent):
        """
        Initial route and client sets are empty. New routes are registered with add_route.
        New clients are registered/deregistered with register/deregister

        Messages are routed by packet type. All port agent endpoints will receive the Router.got_data
        callback on initialization. When data is received at an endpoint it will be used to generate a Packet
        which will be passed to got_data.

        All messages will be routed to all clients registered for a specific packet type. A special packet type
        of ALL will indicate that a client wishes to receive all messages.

        The data_format argument to add_route will determine the format of the message passed to the endpoint.
        A value of PACKET indicates the entire packet should be sent (packed), RAW indicates just the raw data
        will be passed and ASCII indicates the packet should be formatted in a method suitable for logging.
        """
        self.agent = agent
        self.routes = {}
        self.clients = {}
        self.producers = set()
        self.statistics = defaultdict(Counter)
        for packet_type in PacketType.values():
            self.routes[packet_type] = set()
        for endpoint_type in EndpointType.values():
            self.clients[endpoint_type] = set()

        self.stats_time = None

        stats_task = task.LoopingCall(self._log_stats)
        stats_task.start(interval=ROUTER_STATS_INTERVAL)

    def add_route(self, packet_type, endpoint_type, data_format=Format.RAW):
        """
        Route packets of packet_type to all endpoints of endpoint_type using data_format
        """
        self.statistics[endpoint_type][RouterStat.ADD_ROUTE] += 1
        if packet_type == PacketType.ALL:
            for packet_type in PacketType.values():
                log.msg('ADD ROUTE: %s -> %s data_format: %s' %
                        (packet_type, endpoint_type, data_format), logLevel=logging.DEBUG)
                self.routes[packet_type].add((endpoint_type, data_format))
        else:
            log.msg('ADD ROUTE: %s -> %s data_format: %s'
                    % (packet_type, endpoint_type, data_format), logLevel=logging.DEBUG)
            self.routes[packet_type].add((endpoint_type, data_format))

    def got_data(self, packets):
        """
        Asynchronous callback from an endpoint. Packet will be routed as specified in the routing table.
        """
        for packet in packets:
            # delay formatting as string until needed
            # to avoid doing this work on antelope unless
            # an ascii logger is connected
            format_map = {
                Format.RAW: packet.payload,
                Format.PACKET: repr(packet),
                Format.ASCII: None,
            }

            for endpoint_type, data_format in self.routes.get(packet.header.packet_type, []):
                self.statistics[endpoint_type][RouterStat.PACKET_IN] += 1
                self.statistics[endpoint_type][RouterStat.BYTES_IN] += packet.header.packet_size
                for client in self.clients[endpoint_type]:
                    # create the ASCII format now if we actually need it
                    if data_format == Format.ASCII and format_map[Format.ASCII] is None:
                        format_map[Format.ASCII] = str(packet) + NEWLINE
                    self.statistics[endpoint_type][RouterStat.PACKET_OUT] += 1
                    self.statistics[endpoint_type][RouterStat.BYTES_OUT] += packet.header.packet_size
                    client.write(format_map[data_format])

    def register(self, endpoint_type, source):
        """
        Register an endpoint.
        :param endpoint_type value of EndpointType enumeration
        :param source endpoint object, must contain a "write" method
        """
        self.statistics[endpoint_type][RouterStat.ADD_CLIENT] += 1
        log.msg('REGISTER: %s %s' % (endpoint_type, source))
        self.clients[endpoint_type].add(source)

    def deregister(self, endpoint_type, source):
        """
        Deregister an endpoint that has been closed.
        :param endpoint_type value of EndpointType enumeration
        :param source endpoint object, must contain a "write" method
        """
        self.statistics[endpoint_type][RouterStat.DEL_CLIENT] += 1
        log.msg('DEREGISTER: %s %s' % (endpoint_type, source))
        self.clients[endpoint_type].remove(source)

    def _publish_stats(self, stats):
        self.agent.stats_publisher.publish(json.dumps(stats))

    def _log_stats(self):
        start_time = self.stats_time
        self.stats_time = time.time()

        if start_time is not None:
            client_stats = self.statistics.get(EndpointType.CLIENT)
            if client_stats:
                adds = client_stats.get(RouterStat.ADD_CLIENT, 0)
                bytes_in = client_stats.get(RouterStat.BYTES_IN, 0)
                bytes_out = client_stats.get(RouterStat.BYTES_OUT, 0)
                num_clients = {k: len(self.clients[k]) for k in self.clients}
                stats = {
                    'bytes_in': bytes_in,
                    'bytes_out': bytes_out,
                    'num_clients': num_clients,
                    'elapsed': self.stats_time - start_time,
                    'end_time': self.stats_time,
                    'reference_designator': self.agent.refdes,
                    'adds': adds
                }
                self._publish_stats(stats)

        for endpoint, stats in self.statistics.iteritems():
            interval = float(ROUTER_STATS_INTERVAL)
            in_rate = stats[RouterStat.PACKET_IN] / interval
            out_rate = stats[RouterStat.PACKET_OUT] / interval
            in_byte_rate = stats[RouterStat.BYTES_IN] / interval
            out_byte_rate = stats[RouterStat.BYTES_OUT] / interval
            log.msg('Router stats::%s:: (REG) IN: %d OUT: %d' % (
                endpoint,
                stats[RouterStat.ADD_CLIENT],
                stats[RouterStat.DEL_CLIENT],
            ), logLevel=logging.DEBUG)
            log.msg('Router stats::%s:: (PACKETS) IN: %d (%.2f/s) OUT: %d (%.2f/s)' % (
                endpoint,
                stats[RouterStat.PACKET_IN],
                in_rate,
                stats[RouterStat.PACKET_OUT],
                out_rate,
            ), logLevel=logging.DEBUG)
            log.msg('Router stats::%s:: (KB) IN: %d (%.2f/s) OUT: %d (%.2f/s)' % (
                endpoint,
                stats[RouterStat.BYTES_IN] / 1000,
                in_byte_rate / 1000,
                stats[RouterStat.BYTES_OUT] / 1000,
                out_byte_rate / 1000,
            ), logLevel=logging.DEBUG)
        self.statistics.clear()
