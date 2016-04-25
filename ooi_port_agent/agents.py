from __future__ import division

import httplib
import json
from functools import partial

from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor
from twisted.python import log
from twisted.python.logfile import DailyLogFile

import ooi_port_agent
from common import EndpointType
from common import PacketType
from common import Format
from common import HEARTBEAT_INTERVAL
from common import NEWLINE
from factories import DataFactory
from factories import CommandFactory
from factories import InstrumentClientFactory
from factories import DigiInstrumentClientFactory
from factories import DigiCommandClientFactory
from ooi_port_agent.web import get, put
from packet import Packet
from router import Router


#################################################################################
# Port Agents
#
# The default port agents include TCP, RSN, BOTPT and Datalog
# other port agents (CAMHD, Antelope) may require libraries which may not
# exist on all machines
#################################################################################
class PortAgent(object):
    _agent = 'http://localhost:8500/v1/agent/'

    def __init__(self, config):
        self.config = config
        self.refdes = config.get('refdes', config['type'])
        self.ttl = config['ttl']

        self.router = Router()
        self.connections = set()
        self.clients = set()

        self._register_loggers()
        self._create_routes()
        self._start_servers()
        self._heartbeat()
        self.num_connections = 0
        log.msg('Base PortAgent initialization complete')

    def _register_loggers(self):
        self.data_logger = DailyLogFile('%s.datalog' % self.refdes, '.')
        self.ascii_logger = DailyLogFile('%s.log' % self.refdes, '.')
        self.router.register(EndpointType.DATALOGGER, self.data_logger)
        self.router.register(EndpointType.LOGGER, self.ascii_logger)

    def _create_routes(self):
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

    @staticmethod
    def done(response, caller=''):
        if response.code != httplib.OK:
            log.msg(caller + 'http response: %s' % response.code)

    def get_service_name_id(self, service):
        base_id = 'port-agent'
        if service != 'data':
            name = service + '-' + base_id
        else:
            name = base_id

        service_id = name + '-' + self.refdes
        return name, service_id

    def got_port_cb(self, service, port_obj):
        ipaddr = port_obj.getHost()
        port = ipaddr.port
        host = ipaddr.host
        self.config['ports'][service] = port
        self.config['host'] = host
        name, service_id = self.get_service_name_id(service)

        values = {
            'ID': service_id,
            'Name': name,
            'Port': port,
            'Check': {'TTL': '%ss' % self.ttl},
            'Tags': [self.refdes]
        }
        d = put(self._agent + 'service/register', json.dumps(values))
        d.addCallback(self.done, caller='got_port_cb: %s' % service)
        d.addErrback(log.msg, 'Error registering %s port' % service)

        log.msg('got_port_cb: %s %s' % (service, port))

    def _start_servers(self):
        self.data_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.data_endpoint.listen(
            DataFactory(self, PacketType.FROM_DRIVER, EndpointType.CLIENT)
        ).addCallback(partial(self.got_port_cb, 'data'))

        self.command_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.command_endpoint.listen(
            CommandFactory(self, PacketType.PA_COMMAND,EndpointType.COMMAND)
        ).addCallback(partial(self.got_port_cb, 'command'))

        self.sniff_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.sniff_endpoint.listen(
            DataFactory(self, PacketType.UNKNOWN, EndpointType.LOGGER)
        ).addCallback(partial(self.got_port_cb, 'sniff'))

        self.da_endpoint = TCP4ServerEndpoint(reactor, 0)
        self.da_endpoint.listen(
            DataFactory(self, PacketType.FROM_DRIVER, EndpointType.RAW)
        ).addCallback(partial(self.got_port_cb, 'da'))

    def _heartbeat(self):
        packets = Packet.create('HB', PacketType.PA_HEARTBEAT)
        self.router.got_data(packets)

        # Set TTL Check Status
        check_string = self._agent + 'check/pass/service:'

        for service in self.config['ports']:
            name, service_id = self.get_service_name_id(service)
            d = get(check_string + service_id)
            d.addCallback(self.done, caller='TTL check status: %s' % service_id)
            d.addErrback(log.msg, 'Error sending check: %s' % service_id)

        reactor.callLater(HEARTBEAT_INTERVAL, self._heartbeat)

    def client_connected(self, connection):
        log.msg('CLIENT CONNECTED FROM ', connection)
        self.clients.add(connection)

    def client_disconnected(self, connection):
        self.clients.remove(connection)
        log.msg('CLIENT DISCONNECTED FROM ', connection)

    def instrument_connected(self, connection):
        log.msg('CONNECTED TO ', connection)
        self.connections.add(connection)
        if len(self.connections) == self.num_connections:
            self.router.got_data(Packet.create('CONNECTED', PacketType.PA_STATUS))

    def instrument_disconnected(self, connection):
        self.connections.remove(connection)
        log.msg('DISCONNECTED FROM ', connection)
        self.router.got_data(Packet.create('DISCONNECTED', PacketType.PA_STATUS))

    def register_commands(self, command_protocol):
        log.msg('PortAgent register commands for protocol: %s' % command_protocol)
        command_protocol.register_command('get_state', self.get_state)
        command_protocol.register_command('get_config', self.get_config)
        command_protocol.register_command('get_version', self.get_version)

    def get_state(self, *args):
        log.msg('get_state: %r %d' % (self.connections, self.num_connections))
        if len(self.connections) == self.num_connections:
            return Packet.create('CONNECTED', PacketType.PA_STATUS)
        return Packet.create('DISCONNECTED', PacketType.PA_STATUS)

    def get_config(self, *args):
        return Packet.create(json.dumps(self.config), PacketType.PA_CONFIG)

    def get_version(self, *args):
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
        factory = InstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_port, factory)


class RsnPortAgent(TcpPortAgent):
    digi_commands = ['help', 'tinfo', 'cinfo', 'time', 'timestamp', 'power', 'break', 'gettime', 'getver']

    def __init__(self, config):
        super(RsnPortAgent, self).__init__(config)
        self.inst_cmd_port = config['digiport']
        self._start_inst_command_connection()
        self.num_connections = 2
        log.msg('RsnPortAgent initialization complete')

    def _start_inst_connection(self):
        factory = DigiInstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_port, factory)

    def _start_inst_command_connection(self):
        factory = DigiCommandClientFactory(self, PacketType.DIGI_RSP, EndpointType.DIGI)
        reactor.connectTCP(self.inst_addr, self.inst_cmd_port, factory)

    def register_commands(self, command_protocol):
        super(RsnPortAgent, self).register_commands(command_protocol)
        for command in self.digi_commands:
            command_protocol.register_command(command, self._handle_digi_command)

    def _handle_digi_command(self, command, *args):
        command = [command] + list(args)
        return Packet.create(' '.join(command) + NEWLINE, PacketType.DIGI_CMD)


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
        rx_factory = InstrumentClientFactory(self, PacketType.FROM_INSTRUMENT, EndpointType.INSTRUMENT_DATA)
        tx_factory = InstrumentClientFactory(self, PacketType.UNKNOWN, EndpointType.INSTRUMENT)
        reactor.connectTCP(self.inst_addr, self.inst_rx_port, rx_factory)
        reactor.connectTCP(self.inst_addr, self.inst_tx_port, tx_factory)
