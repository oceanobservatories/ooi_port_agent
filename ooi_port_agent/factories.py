from twisted.internet.protocol import Factory
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.python import log

from common import MAX_RECONNECT_DELAY
from protocols import CommandProtocol
from protocols import DigiCommandProtocol
from protocols import DigiInstrumentProtocol
from protocols import InstrumentProtocol
from protocols import PortAgentClientProtocol


#################################################################################
# Factories
#################################################################################


class InstrumentClientFactory(ReconnectingClientFactory):
    """
    Factory for instrument connections. Uses automatic reconnection with exponential backoff.
    """
    protocol = InstrumentProtocol
    maxDelay = MAX_RECONNECT_DELAY

    def __init__(self, port_agent, packet_type, endpoint_type):
        self.port_agent = port_agent
        self.packet_type = packet_type
        self.endpoint_type = endpoint_type
        self.connection = None

    def buildProtocol(self, addr):
        log.msg('Made TCP connection to instrument (%s), building protocol' % addr)
        p = self.protocol(self.port_agent, self.packet_type, self.endpoint_type)
        p.factory = self
        self.connection = p
        self.resetDelay()
        return p


class DigiCommandClientFactory(InstrumentClientFactory):
    """
    Overridden to use DigiProtocol to automatically set binary timestamp on connection
    """
    protocol = DigiCommandProtocol


class DigiInstrumentClientFactory(InstrumentClientFactory):
    """
    Overridden to use DigiInstrumentProtocol to pass port agent packets through unchanged
    """
    protocol = DigiInstrumentProtocol


class DataFactory(Factory):
    """
    This is the base class for incoming connections (data, command, sniffer)
    """
    protocol = PortAgentClientProtocol

    def __init__(self, port_agent, packet_type, endpoint_type):
        self.port_agent = port_agent
        self.packet_type = packet_type
        self.endpoint_type = endpoint_type

    def buildProtocol(self, addr):
        log.msg('Established incoming connection (%s)' % addr)
        p = self.protocol(self.port_agent, self.packet_type, self.endpoint_type)
        p.factory = self
        return p


class CommandFactory(DataFactory):
    """
    Subclasses DataFactory to utilize the CommandProtocol for incoming command connections
    """
    protocol = CommandProtocol

    def __init__(self, port_agent, packet_type, endpoint_type):
        DataFactory.__init__(self, port_agent, packet_type, endpoint_type)

    def buildProtocol(self, addr):
        log.msg('Established incoming command connection (%s), building protocol' % addr)
        p = self.protocol(self.port_agent, self.packet_type, self.endpoint_type)
        p.factory = self
        self.port_agent.register_commands(p)
        return p
