from __future__ import division
import glob

import ooi_port_agent
import json
import re

from datetime import datetime
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from common import EndpointType, PacketType, Format, HEARTBEAT_INTERVAL, NEWLINE, string_to_ntp_date_time
from factories import DataFactory, CommandFactory, InstrumentClientFactory, DigiInstrumentClientFactory, \
    DigiCommandClientFactory
from packet import Packet, PacketHeader
from router import Router


#################################################################################
# Port Agents
#
# The default port agents include TCP, RSN, BOTPT and Datalog
# other port agents (CAMHD, Antelope) may require libraries which may not
# exist on all machines
#################################################################################

class PortAgent(object):
    def __init__(self, config):
        self.config = config
        self.data_port = config['port']
        self.command_port = config['commandport']
        self.sniff_port = config['sniffport']
        self.name = config.get('name', str(self.command_port))

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
        self.data_logger = DailyLogFile('%s.datalog' % self.name, '.')
        self.ascii_logger = DailyLogFile('%s.log' % self.name, '.')
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

    def _start_servers(self):
        self.data_endpoint = TCP4ServerEndpoint(reactor, self.data_port)
        self.data_endpoint.listen(DataFactory(self, PacketType.FROM_DRIVER, EndpointType.CLIENT))

        self.command_endpoint = TCP4ServerEndpoint(reactor, self.command_port)
        self.command_endpoint.listen(CommandFactory(self, PacketType.PA_COMMAND, EndpointType.COMMAND))

        if self.sniff_port:
            self.sniff_port = int(self.sniff_port)
            self.sniff_endpoint = TCP4ServerEndpoint(reactor, self.sniff_port)
            self.sniff_endpoint.listen(DataFactory(self, PacketType.UNKNOWN, EndpointType.LOGGER))
        else:
            self.sniff_endpoint = None

    def _heartbeat(self):
        packets = Packet.create('HB', PacketType.PA_HEARTBEAT)
        self.router.got_data(packets)
        reactor.callLater(HEARTBEAT_INTERVAL, self._heartbeat)

    def client_connected(self, connection):
        log.msg('CLIENT CONNECTED FROM ', connection)
        self.clients.add(connection)
        if len(self.clients) == self.num_connections:
            self.router.got_data(Packet.create('CONNECTED', PacketType.PA_STATUS))

    def client_disconnected(self, connection):
        self.clients.remove(connection)
        log.msg('CLIENT DISCONNECTED FROM ', connection)
        self.router.got_data(Packet.create('DISCONNECTED', PacketType.PA_STATUS))

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


class DatalogReadingPortAgent(PortAgent):
    def __init__(self, config):
        super(DatalogReadingPortAgent, self).__init__(config)
        self.files = []
        for each in config['files']:
            self.files.extend(glob.glob(each))

        self.files.sort()
        self._filehandle = None
        self.target_types = [PacketType.FROM_INSTRUMENT, PacketType.PA_CONFIG]
        self._start_when_ready()

    def _register_loggers(self):
        """
        Overridden, no logging when reading a datalog...
        """
        pass

    def _start_when_ready(self):
        log.msg('waiting for a client connection', self.router.clients[EndpointType.INSTRUMENT_DATA])
        if len(self.router.clients[EndpointType.CLIENT]) > 0:
            self._read()
        else:
            reactor.callLater(1.0, self._start_when_ready)

    def _read(self):
        """
        Read one packet, publish if appropriate, then return.
        We must not read all packets in a loop here, or we will not actually publish them until the end...
        """
        if self._filehandle is None and not self.files:
            log.msg('Completed reading specified port agent logs, exiting...')
            reactor.stop()
            return

        if self._filehandle is None:
            name = self.files.pop(0)
            log.msg('Begin reading:', name)
            self._filehandle = open(name, 'r')

        packet = Packet.packet_from_fh(self._filehandle)
        if packet is not None:
            if packet.header.packet_type in self.target_types:
                self.router.got_data([packet])

        else:
            self._filehandle.close()
            self._filehandle = None

        # allow the reactor loop to process other events
        reactor.callLater(0.01, self._read)


class DigiDatalogAsciiPortAgent(DatalogReadingPortAgent):
    def __init__(self, config):
        super(DigiDatalogAsciiPortAgent, self).__init__(config)
        self.ooi_ts_regex = re.compile(r'<OOI-TS (.+?) [TX][NS]>\r\n(.*?)<\\OOI-TS>', re.DOTALL)
        self.buffer = ''
        self.MAXBUF = 65535

        # special case for RSN archived data
        # if all files have date_UTC in filename then sort by that
        def search_utc(f):
            match = re.search('(\d+T\d+_UTC)', f)
            if match is None:
                return None
            else:
                return match.group(1)

        if all((search_utc(f) for f in self.files)):
            self.files.sort(key=search_utc)

    def _read(self):
        """
        Read a chunk of data and inspect it for a complete DIGI ASCII record. When found, publish.
        """
        if self._filehandle is None and not self.files:
            log.msg('Completed reading specified port agent logs')
            return

        if self._filehandle is None:
            name = self.files.pop(0)
            log.msg('Begin reading:', name)
            self._filehandle = open(name, 'r')

        chunk = self._filehandle.read(1024)
        if chunk != '':
            self.buffer += chunk
            new_index = 0
            for match in self.ooi_ts_regex.finditer(self.buffer):
                payload = match.group(2)
                try:
                    packet_time = string_to_ntp_date_time(match.group(1))
                    header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT, payload_size=len(payload), packet_time=packet_time)
                    header.set_checksum(payload)
                    packet = Packet(payload=payload, header=header)
                    self.router.got_data([packet])
                except ValueError:
                    log.err('Unable to extract timestamp from record: %r' % match.group())
                new_index = match.end()

            if new_index > 0:
                self.buffer = self.buffer[new_index:]

            if len(self.buffer) > self.MAXBUF:
                self.buffer = self.buffer[-self.MAXBUF:]

        else:
            self._filehandle.close()
            self._filehandle = None

        # allow the reactor loop to process other events
        reactor.callLater(0.01, self._read)


class ChunkyDatalogPortAgent(DatalogReadingPortAgent):
    def __init__(self, config):
        super(ChunkyDatalogPortAgent, self).__init__(config)

    def _read(self):
        """
        Read one line at a time, publish as a packet with TS of 0
        It is expected that the driver will use the internal timestamp
        of the record as the definitive timestamp
        """
        if self._filehandle is None and not self.files:
            log.msg('Completed reading specified port agent logs, exiting...')
            reactor.stop()
            return

        if self._filehandle is None:
            name = self.files.pop(0)
            log.msg('Begin reading:', name)
            self._filehandle = open(name, 'r')

        data = self._filehandle.read(1024)
        if data != '':
            header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT, payload_size=len(data), packet_time=0)
            header.set_checksum(data)
            packet = [Packet(payload=data, header=header)]
            self.router.got_data(packet)

        else:
            self._filehandle.close()
            self._filehandle = None

        # allow the reactor loop to process other events
        reactor.callLater(0.01, self._read)

