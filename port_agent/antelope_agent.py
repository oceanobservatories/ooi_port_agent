import threading
from twisted.internet import reactor
from twisted.python import log
from agents import PortAgent
from antelope import Pkt
from antelope.orb import orbopen, OrbIncompleteException, ORBOLDEST
from common import PacketType
from packet import Packet
import msgpack


class OrbThread(threading.Thread):
    def __init__(self, addr, port, port_agent):
        super(OrbThread, self).__init__()
        self.addr = addr
        self.port = port
        self.port_agent = port_agent

    def run(self):
        with orbopen('%s:%d' % (self.addr, self.port)) as orb:
            while self.port_agent.keep_going:
                try:
                    pktid, srcname, pkttime, data = orb.reap(1)
                    orb_packet = Pkt.Packet(srcname, pkttime, data)
                    packets = create_packets(orb_packet)
                    reactor.callFromThread(self.port_agent.router.got_data, packets)
                except OrbIncompleteException:
                    pass


class AntelopePortAgent(PortAgent):
    def __init__(self, config):
        super(AntelopePortAgent, self).__init__(config)
        self.inst_addr = config['instaddr']
        self.inst_port = config['instport']
        self.keep_going = False
        self.select = []
        self.seek = ORBOLDEST
        self.orb_thread = None
        reactor.addSystemEventTrigger('before', 'shutdown', self._orb_stop())

    def _register_loggers(self):
        """
        Overridden, no logging on antelope, antelope keeps track of its own data...
        """
        # TODO: verify

    def register_commands(self, command_protocol):
        super(AntelopePortAgent, self).register_commands(command_protocol)
        log.msg('PortAgent register commands for protocol: %s' % command_protocol)
        command_protocol.register_command('orbselect', self._set_select)
        command_protocol.register_command('orbseek', self._set_seek)
        command_protocol.register_command('orbstart', self._orb_start)
        command_protocol.register_command('orbstop', self._orb_stop)

    def _set_select(self, command, *args):
        if len(args) == 0:
            self.select = []
        else:
            self.select.extend(args)

    def _set_seek(self, command, *args):
        if len(args) == 0:
            self.seek = ORBOLDEST
        else:
            self.seek = int(args[1])

    def _orb_start(self, *args):
        if self.orb_thread is None:
            self.keep_going = True
            self.orb_thread = OrbThread(self.inst_addr, self.inst_port, self)
            self.orb_thread.start()

    def _orb_stop(self, *args):
        self.keep_going = False
        if self.orb_thread is not None:
            self.orb_thread.destroy()


def create_packets(orb_packet):
    packets = []
    for channel in orb_packet.channels:
        d = {'calib': channel.calib,
             'calper': channel.calper,
             'net': channel.net,
             'sta': channel.sta,
             'chan': channel.chan,
             'data': channel.data,
             'nsamp': channel.data,
             'samprate': channel.samprate,
             'time': channel.time,
             'type_suffix': orb_packet.type.suffix,
             'version': orb_packet.version,
             }

        packets.extend(Packet.create(msgpack.packb(d), PacketType.FROM_INSTRUMENT))
    return packets

