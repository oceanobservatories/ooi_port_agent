import threading
import time
from twisted.internet import reactor
from twisted.python import log
from agents import PortAgent
from antelope import Pkt
from antelope.orb import orbopen, OrbIncompleteException, ORBOLDEST
from common import PacketType, NEWLINE
from packet import Packet
import cPickle as pickle


class OrbThread(threading.Thread):
    def __init__(self, addr, port, port_agent):
        super(OrbThread, self).__init__()
        self.addr = addr
        self.port = port
        self.port_agent = port_agent

    def run(self):
        with orbopen('%s:%d' % (self.addr, self.port)) as orb:
            for source in self.port_agent.select:
                orb.select(source)
            orb.seek(self.port_agent.seek)
            while self.port_agent.keep_going:
                try:
                    pktid, srcname, pkttime, data = orb.reap(1)
                    orb_packet = Pkt.Packet(srcname, pkttime, data)
                    packets = create_packets(orb_packet, pktid)
                    reactor.callFromThread(self.port_agent.router.got_data, packets)
                    time.sleep(.00001)
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
        reactor.addSystemEventTrigger('before', 'shutdown', self._orb_stop)

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
            msg = 'Cleared orb select list'
        else:
            self.select.extend(args)
            msg = 'Added to select list: %s' % args
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def _set_seek(self, command, *args):
        if len(args) == 0:
            self.seek = ORBOLDEST
        else:
            self.seek = int(args[0])
        msg = 'Orb seek set to %s' % self.seek
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def _orb_start(self, *args):
        if self.orb_thread is None:
            self.keep_going = True
            self.orb_thread = OrbThread(self.inst_addr, self.inst_port, self)
            self.orb_thread.start()
            msg = 'Started orb thread with select: %s seek: %s' % (self.select, self.seek)
        else:
            msg = 'Orb already running!'
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def _orb_stop(self, *args):
        self.keep_going = False
        if self.orb_thread is not None:
            self.orb_thread.join()
            self.orb_thread = None
            msg = 'Stopped orb thread'
        else:
            msg = 'Orb thread not running!'
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def get_state(self, *args):
        if self.orb_thread is not None:
            msg = 'CONNECTED'
        else:
            msg = 'DISCONNECTED'
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)


def create_packets(orb_packet, pktid):
    packets = []
    for channel in orb_packet.channels:
        d = {'calib': channel.calib,
             'calper': channel.calper,
             'net': channel.net,
             'sta': channel.sta,
             'chan': channel.chan,
             'data': channel.data,
             'nsamp': channel.nsamp,
             'samprate': channel.samprate,
             'time': channel.time,
             'type_suffix': orb_packet.type.suffix,
             'version': orb_packet.version,
             'pktid': pktid,
             }

        packets.extend(Packet.create(pickle.dumps(d, protocol=-1), PacketType.FROM_INSTRUMENT))
    return packets

