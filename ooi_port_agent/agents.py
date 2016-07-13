from __future__ import division

import httplib
import json
from functools import partial

from twisted.internet import reactor, task
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from twisted.web.client import readBody

import ooi_port_agent
from common import EndpointType, BINARY_TIMESTAMP
from common import Format
from common import HEARTBEAT_INTERVAL
from common import NEWLINE
from common import PacketType
from factories import CommandFactory
from factories import DataFactory
from factories import DigiInstrumentClientFactory
from factories import InstrumentClientFactory
from ooi_port_agent.protocols import DigiCommandProtocol
from ooi_port_agent.statistics import StatisticsPublisher
from ooi_port_agent.web import get, put
from packet import Packet
from router import Router


CONSUL_RETRY_INTERVAL = 10
INSTRUMENT_DISCONNECT_FORGIVENESS_INTERVAL = 10


#################################################################################
# Port Agents
#
# The default port agents include TCP, RSN, BOTPT and Datalog
# other port agents (CAMHD, Antelope) may require libraries which may not
# exist on all machines
#################################################################################
# noinspection PyUnusedLocal
class PortAgent(object):
    _agent = 'http://localhost:8500/v1/agent/'

    def __init__(self, config):
        self.config = config
        self.refdes = config.get('refdes', config['type'])
        self.ttl = config['ttl']
        self.num_connections = 0
        self.disconnect_notification_id = None

        self.router = Router(self)
        self.stats_publisher = StatisticsPublisher()
        self.stats_publisher.connect()
        self.connections = set()
        self.clients = set()

        self._register_loggers()
        self._create_routes()
        self._start_servers()
        self.get_consul_host()

        heartbeat_task = task.LoopingCall(self._heartbeat)
        heartbeat_task.start(interval=HEARTBEAT_INTERVAL)

        log.msg('Base PortAgent initialization complete')

    def _register_loggers(self):
        """
        Create and register the binary and ascii loggers
        :return:
        """
        self.data_logger = DailyLogFile('%s.datalog' % self.refdes, '.')
        self.ascii_logger = DailyLogFile('%s.log' % self.refdes, '.')
        self.router.register(EndpointType.DATALOGGER, self.data_logger)
        self.router.register(EndpointType.LOGGER, self.ascii_logger)

    def _create_routes(self):
        """
        Default routes for all port agent types
        This provides a mapping from packet type to endpoint type
        :return:
        """
        # Register the logger and datalogger to receive all messages
        self.router.add_route(PacketType.ALL, EndpointType.LOGGER, data_format=Format.ASCII)
        self.router.add_route(PacketType.ALL, EndpointType.DATALOGGER, data_format=Format.PACKET)

        # from DRIVER
        self.router.add_route(PacketType.FROM_DRIVER, EndpointType.INSTRUMENT, data_format=Format.RAW)

        # from INSTRUMENT
        self.router.add_route(PacketType.FROM_INSTRUMENT, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.FROM_INSTRUMENT, EndpointType.RAW, data_format=Format.RAW)
        self.router.add_route(PacketType.PICKLED_FROM_INSTRUMENT, EndpointType.CLIENT, data_format=Format.PACKET)

        # from COMMAND SERVER
        self.router.add_route(PacketType.PA_COMMAND, EndpointType.COMMAND_HANDLER, data_format=Format.PACKET)

        # from PORT_AGENT
        self.router.add_route(PacketType.PA_CONFIG, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.PA_CONFIG, EndpointType.COMMAND, data_format=Format.RAW)
        self.router.add_route(PacketType.PA_FAULT, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.PA_HEARTBEAT, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.PA_STATUS, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.PA_STATUS, EndpointType.COMMAND, data_format=Format.RAW)

        # from COMMAND HANDLER
        self.router.add_route(PacketType.DIGI_CMD, EndpointType.DIGI, data_format=Format.RAW)

        # from DIGI
        self.router.add_route(PacketType.DIGI_RSP, EndpointType.CLIENT, data_format=Format.PACKET)
        self.router.add_route(PacketType.DIGI_RSP, EndpointType.COMMAND, data_format=Format.RAW)

    def get_service_name_id(self, service):
        """
        Given a service name, compute the fully qualified service name and id for this agent
        :param service: service name
        :return:
        """
        base_id = 'port-agent'
        if service != 'data':
            name = service + '-' + base_id
        else:
            name = base_id

        service_id = name + '-' + self.refdes
        return name, service_id

    @inlineCallbacks
    def got_port_cb(self, service, port_obj):
        """
        Called after a port is successfully bound by the server.
        Register the service with Consul.
        :param service: service to be registered
        :param port_obj: port object containing the IP address and port
        :return:
        """
        ipaddr = port_obj.getHost()
        port = ipaddr.port
        self.config['ports'][service] = port
        name, service_id = self.get_service_name_id(service)

        values = {
            'ID': service_id,
            'Name': name,
            'Port': port,
            'Check': {'TTL': '%ss' % self.ttl},
            'Tags': [self.refdes]
        }

        response = yield put(self._agent + 'service/register', json.dumps(values))
        if response.code != httplib.OK:
            log.msg(service + 'http response: %s' % response.code)

    def _start_servers(self):
        """
        Start the base port agent servers:
        DATA
        COMMAND
        SNIFF
        :return:
        """
        self.data_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.data_endpoint.listen(
            DataFactory(self, PacketType.FROM_DRIVER, EndpointType.CLIENT)
        ).addCallback(partial(self.got_port_cb, 'data'))

        self.command_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.command_endpoint.listen(
            CommandFactory(self, PacketType.PA_COMMAND, EndpointType.COMMAND)
        ).addCallback(partial(self.got_port_cb, 'command'))

        self.sniff_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.sniff_endpoint.listen(
            DataFactory(self, PacketType.UNKNOWN, EndpointType.LOGGER)
        ).addCallback(partial(self.got_port_cb, 'sniff'))

    @inlineCallbacks
    def _heartbeat(self):
        """
        Send a heartbeat packet and set our Consul status to PASS
        :return:
        """
        packets = Packet.create('HB', PacketType.PA_HEARTBEAT)
        self.router.got_data(packets)

        # Set TTL Check Status
        check_string = self._agent + 'check/pass/service:'

        for service in self.config['ports']:
            name, service_id = self.get_service_name_id(service)
            yield get(check_string + service_id)

    @inlineCallbacks
    def get_consul_host(self):
        """
        Fetch our host information from Consul
        :return:
        """
        url = self._agent + 'self'
        try:
            response = yield get(url)
            body = yield readBody(response)
            data = json.loads(body)
            host = data['Member']['Addr']
            self.config['host'] = host
        except (ConnectionRefusedError, KeyError) as e:
            log.msg('Unable to retrieve host address from consul: ', e)
            reactor.callLater(CONSUL_RETRY_INTERVAL, self.get_consul_host)

    def client_connected(self, connection):
        """
        Log an incoming client connection
        :param connection: connection object
        :return:
        """
        log.msg('CLIENT CONNECTED FROM ', connection)
        self.clients.add(connection)

    def client_disconnected(self, connection):
        """
        Log a client disconnect
        :param connection: connection object
        :return:
        """
        self.clients.remove(connection)
        log.msg('CLIENT DISCONNECTED FROM ', connection)

    def instrument_connected(self, connection):
        """
        Log a successful instrument connection,
        notifying the driver if the target number of connections is reached.
        :param connection: connection object
        :return:
        """
        log.msg('CONNECTED TO ', connection)
        self.connections.add(connection)
        if len(self.connections) == self.num_connections:
            # We are now CONNECTED
            # Cancel any pending DISCONNECTED notification
            if self.disconnect_notification_id is not None:
                self.disconnect_notification_id.cancel()
            # Notify driver we are CONNECTED
            self.router.got_data(Packet.create('CONNECTED', PacketType.PA_STATUS))

    def instrument_disconnected(self, connection):
        """
        Log a instrument disconnect,
        notifying the driver after INSTRUMENT_DISCONNECT_FORGIVENESS_INTERVAL
        if the target number of connections is not reached.
        :param connection: connection object
        :return:
        """
        self.connections.remove(connection)
        log.msg('DISCONNECTED FROM ', connection)

        if self.disconnect_notification_id is None:
            self.disconnect_notification_id = reactor.callLater(INSTRUMENT_DISCONNECT_FORGIVENESS_INTERVAL,
                                                                self.notify_disconnected)

    def notify_disconnected(self):
        """
        Send a disconnect notification to the instrument driver
        :return:
        """
        log.msg('Notifying driver we are DISCONNECTED')
        self.router.got_data(Packet.create('DISCONNECTED', PacketType.PA_STATUS))

    def register_commands(self, command_protocol):
        """
        Register the available commands for this agent on a new CommandProtocol object
        :param command_protocol: CommandProtocol object
        :return:
        """
        log.msg('PortAgent register commands for protocol: %s' % command_protocol)
        command_protocol.register_command('get_state', self.get_state)
        command_protocol.register_command('get_config', self.get_config)
        command_protocol.register_command('get_version', self.get_version)

    def get_state(self, *args):
        """
        Respond to an incoming get_state request
        :param args: not applicable to this command
        :return:
        """
        log.msg('get_state: %r %d' % (self.connections, self.num_connections))
        if len(self.connections) == self.num_connections:
            return Packet.create('CONNECTED', PacketType.PA_STATUS)
        return Packet.create('DISCONNECTED', PacketType.PA_STATUS)

    def get_config(self, *args):
        """
        Respond to an incoming get_config request
        :param args: not applicable to this command
        :return:
        """
        return Packet.create(json.dumps(self.config), PacketType.PA_CONFIG)

    # noinspection PyMethodMayBeStatic
    def get_version(self, *args):
        """
        Respond to an incoming get_version request
        :param args: not applicable to this command
        :return:
        """
        return Packet.create(ooi_port_agent.__version__, PacketType.PA_CONFIG)


class TcpPortAgent(PortAgent):
    """
    Make a single TCP connection to an instrument.
    Data from the instrument connection is routed to all connected clients.
    Data from the client(s) is routed to the instrument connection
    """

    def __init__(self, config):
        super(TcpPortAgent, self).__init__(config)
        self.inst_addr = config['instaddr']
        self.inst_port = config['instport']
        self.num_connections = 1
        self._start_inst_connection()
        log.msg('TcpPortAgent initialization complete')

    def _start_inst_connection(self):
        """
        Create the TCP connection to this agent's instrument
        :return:
        """
        factory = InstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_port, factory)


class RsnPortAgent(TcpPortAgent):
    digi_commands = ['help', 'tinfo', 'cinfo', 'time', 'timestamp', 'power', 'break', 'gettime', 'getver']

    def __init__(self, config):
        super(RsnPortAgent, self).__init__(config)
        self.inst_cmd_port = config['digiport']
        self._set_binary()
        self.num_connections = 1
        log.msg('RsnPortAgent initialization complete')

    def _start_inst_connection(self):
        """
        Create the TCP connection to this agent's DIGI data port
        :return:
        """
        factory = DigiInstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_port, factory)

    @inlineCallbacks
    def _start_inst_command_connection(self):
        """
        Create the TCP connection to this agent's DIGI command port
        :return: DigiCommandProtocol object connected to the DIGI command port
        """
        command_protocol = partial(DigiCommandProtocol, self, PacketType.DIGI_RSP, EndpointType.DIGI)
        protocol = yield ClientCreator(reactor, command_protocol).connectTCP(self.inst_addr, self.inst_cmd_port)
        returnValue(protocol)

    def register_commands(self, command_protocol):
        """
        Register the available commands for this agent on a new CommandProtocol object
        Overrides base class to include DIGI-specific commands.
        :param command_protocol: CommandProtocol object
        :return:
        """
        super(RsnPortAgent, self).register_commands(command_protocol)
        for command in self.digi_commands:
            command_protocol.register_command(command, self._handle_digi_command)

    @inlineCallbacks
    def _handle_digi_command(self, command, *args):
        """
        Request a protocol object connected to the DIGI command port, then
        produce the packets corresponding to a requested digi command
        :param command: command
        :param args: arguments
        :return: List of packets representing this command.
        """
        command = [command] + list(args)
        command = ' '.join(command) + NEWLINE
        protocol = yield self._start_inst_command_connection()
        protocol.write(command)
        returnValue(Packet.create(command, PacketType.DIGI_CMD))

    @inlineCallbacks
    def _set_binary(self):
        """
        Send the digi command to enable binary timestamping.
        :return:
        """
        packets = yield self._handle_digi_command(BINARY_TIMESTAMP)
        self.router.got_data(packets)


class BotptPortAgent(PortAgent):
    """
    Make multiple TCP connection to an instrument (one TX, one RX).
    Data from the instrument RX connection is routed to all connected clients.
    Data from the client(s) is routed to the instrument TX connection
    """

    def __init__(self, config):
        super(BotptPortAgent, self).__init__(config)
        self.inst_rx_port = config['rxport']
        self.inst_tx_port = config['txport']
        self.inst_addr = config['instaddr']
        self._start_inst_connection()
        self.num_connections = 2
        log.msg('BotptPortAgent initialization complete')

    def _start_inst_connection(self):
        """
        Create the TCP connection to this agent's instrument
        Overrides super method to connect to separate RX/TX ports of BOTPT
        :return:
        """
        rx_factory = InstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT_DATA)
        tx_factory = InstrumentClientFactory(self, PacketType.UNKNOWN, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_rx_port, rx_factory)
        reactor.connectTCP(self.inst_addr, self.inst_tx_port, tx_factory)
