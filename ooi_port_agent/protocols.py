#################################################################################
# Protocols
#################################################################################
from collections import deque
import platform
import socket
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import connectionDone
from twisted.protocols.basic import LineOnlyReceiver
from twisted.python import log
from common import PacketType
from common import BINARY_TIMESTAMP
from packet import Packet

KEEPALIVE_IDLE = 100
KEEPALIVE_INTVL = 5


class PortAgentProtocol(Protocol):
    """
    General protocol for the port agent.
    """
    def __init__(self, port_agent, packet_type, endpoint_type):
        self.port_agent = port_agent
        self.packet_type = packet_type
        self.endpoint_type = endpoint_type

    def dataReceived(self, data):
        """
        Called asynchronously when data is received from this connection
        """
        self.port_agent.router.got_data(Packet.create(data, self.packet_type))

    def write(self, data):
        self.transport.write(data)

    def connectionMade(self):
        """
        Register this protocol with the router
        """
        self.port_agent.router.register(self.endpoint_type, self)
        # The default transport buffer size is 2^16 bytes
        # this is about one packet for the broadband hydrophones.
        # We need a deeper buffer to provide decent throughput.
        # The buffer is still actually unbounded, this just defines
        # the threshold for a call to pauseProducing if it is supported
        # by the producer
        self.transport.bufferSize *= 10

    def connectionLost(self, reason=connectionDone):
        """
        Connection lost, deregister with the router
        """
        self.port_agent.router.deregister(self.endpoint_type, self)


class PortAgentClientProtocol(PortAgentProtocol):
    def connectionMade(self):
        """
        Register this protocol with the router and add to the port agent client list
        """
        self.port_agent.router.register(self.endpoint_type, self)
        self.port_agent.client_connected(self)

    def connectionLost(self, reason=connectionDone):
        """
        Connection lost, deregister with the router and remove from the port agent client list
        """
        self.port_agent.router.deregister(self.endpoint_type, self)
        self.port_agent.client_disconnected(self)


class InstrumentProtocol(PortAgentProtocol):
    """
    Overrides PortAgentProtocol for instrument state tracking
    """
    def connectionMade(self):
        self.port_agent.instrument_connected(self)
        self.port_agent.router.register(self.endpoint_type, self)
        self.configure_tcp()

    def connectionLost(self, reason=connectionDone):
        self.port_agent.instrument_disconnected(self)
        self.port_agent.router.deregister(self.endpoint_type, self)

    def configure_tcp(self):
        self.transport.setTcpKeepAlive(True)
        self.transport.setTcpNoDelay(True)

        # configure keepalive
        if platform.system() == 'Darwin':
            TCP_KEEPALIVE = 0x10
            TCP_KEEPINTVL = 0x101
            self.transport.socket.setsockopt(socket.SOL_TCP, TCP_KEEPALIVE, KEEPALIVE_IDLE)
            self.transport.socket.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, KEEPALIVE_INTVL)

        elif platform.system() == 'Linux':
            self.transport.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, KEEPALIVE_IDLE)
            self.transport.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, KEEPALIVE_INTVL)


class DigiInstrumentProtocol(InstrumentProtocol):
    """
    Overrides InstrumentProtocol to automatically send the binary timestamp command on connection
    """
    def __init__(self, port_agent, packet_type, endpoint_type):
        InstrumentProtocol.__init__(self, port_agent, packet_type, endpoint_type)
        self.buffer = deque(maxlen=65535)

    def dataReceived(self, data):
        self.buffer.extend(data)
        data = ''.join(self.buffer)
        while True:
            packet, data = Packet.packet_from_buffer(data)
            if packet is not None:
                self.port_agent.router.got_data([packet])
                self.buffer.clear()
                self.buffer.extend(data)
            else:
                break


class DigiCommandProtocol(InstrumentProtocol):
    """
    Overrides InstrumentProtocol to automatically send the binary timestamp command on connection
    """
    def __init__(self, port_agent, packet_type, endpoint_type):
        InstrumentProtocol.__init__(self, port_agent, packet_type, endpoint_type)

    def connectionMade(self):
        InstrumentProtocol.connectionMade(self)
        self.transport.write(BINARY_TIMESTAMP)


class CommandProtocol(LineOnlyReceiver):
    """
    Specialized protocol which is not called until a line of text terminated by the delimiter received
    """
    delimiter = b'\n'

    def __init__(self, port_agent, packet_type, endpoint_type):
        log.msg('Creating CommandProtocol')
        self.port_agent = port_agent
        self.packet_type = packet_type
        self.endpoint_type = endpoint_type
        self.callbacks = {}

    def register_command(self, command, callback):
        log.msg('Registering callback for command: %s' % command)
        self.callbacks[command] = callback

    def lineReceived(self, line):
        packets = Packet.create(line, self.packet_type)
        self.port_agent.router.got_data(packets)
        self.handle_command(line)

    def handle_command(self, command_line):
        log.msg('handle_command: %s' % command_line)
        parts = command_line.split()
        if len(parts) > 0:
            command = parts[0]
            args = parts[1:]

            if command in self.callbacks:
                packets = self.callbacks[command](command, *args)
            else:
                packets = Packet.create('Received bad command on command port: %r' % command, PacketType.PA_FAULT)

        else:
            packets = Packet.create('Received empty command on command port', PacketType.PA_FAULT)

        if packets:
            self.port_agent.router.got_data(packets)

    def connectionMade(self):
        self.port_agent.router.register(self.endpoint_type, self)

    def connectionLost(self, reason=connectionDone):
        self.port_agent.router.deregister(self.endpoint_type, self)

    def write(self, data):
        self.transport.write(data)
