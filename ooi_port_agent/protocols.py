#################################################################################
# Protocols
#################################################################################
from collections import deque
import json
from twisted.internet.protocol import Protocol, connectionDone
from twisted.protocols.basic import LineOnlyReceiver
from twisted.python import log
from common import PacketType, BINARY_TIMESTAMP
from packet import Packet


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
        self.transport.setTCPKeepAlive(True)
        self.transport.setTCPNoDelay(True)

    def connectionLost(self, reason=connectionDone):
        """
        Connection lost, deregister with the router
        """
        self.port_agent.router.deregister(self.endpoint_type, self)


class InstrumentProtocol(PortAgentProtocol):
    """
    Overrides PortAgentProtocol for instrument state tracking
    """
    def connectionMade(self):
        self.port_agent.instrument_connected(self)
        self.port_agent.router.register(self.endpoint_type, self)

    def connectionLost(self, reason=connectionDone):
        self.port_agent.instrument_disconnected(self)
        self.port_agent.router.deregister(self.endpoint_type, self)


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
        packet, remaining = Packet.packet_from_buffer(data)
        if packet is not None:
            self.port_agent.router.got_data([packet])
            self.buffer.clear()
            self.buffer.extendleft(remaining)


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
    default delimiter is '\r\n'
    """
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

        self.port_agent.router.got_data(packets)

    def connectionMade(self):
        self.port_agent.router.register(self.endpoint_type, self)
        self.transport.setTCPKeepAlive(True)
        self.transport.setTCPNoDelay(True)

    def connectionLost(self, reason=connectionDone):
        self.port_agent.router.deregister(self.endpoint_type, self)

    def write(self, data):
        self.transport.write(data)
