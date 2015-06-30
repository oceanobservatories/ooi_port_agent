import json
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

def get_one(orb):
    try:
        pktid, srcname, pkttime, data = orb.reap(1)
        orb_packet = Pkt.Packet(srcname, pkttime, data)
        return create_packets(orb_packet, pktid)
    except OrbIncompleteException:
        return []


class OrbThread(threading.Thread):
    def __init__(self, orb, port_agent):
        super(OrbThread, self).__init__()
        self.orb = orb
        self.port_agent = port_agent

    def run(self):
        while self.port_agent.keep_going:
            reactor.callFromThread(self.port_agent.router.got_data, get_one(self.orb))
            time.sleep(.1)



class AntelopePortAgent(PortAgent):
    def __init__(self, config):
        super(AntelopePortAgent, self).__init__(config)
        self.inst_addr = config['instaddr']
        self.inst_port = config['instport']
        self.keep_going = False
        self.orb = orbopen('%s:%d' % (self.inst_addr, self.inst_port))
        log.msg('Opened orb: %s' % self.orb)
        self.orb_thread = None
        reactor.addSystemEventTrigger('before', 'shutdown', self._orb_stop)

    def _register_loggers(self):
        """
        Overridden, no logging on antelope, antelope keeps track of its own data...
        """
        # TODO: verify

    def client_disconnected(self, connection):
        """
        Overridden to stop the running orb thread (if running) if all clients disconnect
        """
        super(AntelopePortAgent, self).client_disconnected(connection)
        if len(self.clients) == 0:
            log.msg('All clients disconnected, stopping orb thread if running')
            self._orb_stop()

    def register_commands(self, command_protocol):
        super(AntelopePortAgent, self).register_commands(command_protocol)
        log.msg('PortAgent register commands for protocol: %s' % command_protocol)
        command_protocol.register_command('orblist', self._list_channels)
        command_protocol.register_command('orbselect', self._set_select)
        command_protocol.register_command('orbseek', self._set_seek)
        command_protocol.register_command('orbstart', self._orb_start)
        command_protocol.register_command('orbstop', self._orb_stop)
        command_protocol.register_command('orbget', self._orb_get)

    def _list_channels(self, *args):
        with orbopen('%s:%d' % (self.inst_addr, self.inst_port)) as orb:
            return Packet.create(json.dumps(orb.sources(), indent=1) + NEWLINE, PacketType.PA_STATUS)

    def _set_select(self, command, *args):
        if len(args) == 0:
            num_sources = self.orb.select('')
        else:
            num_sources = self.orb.select(args[0])
        msg = 'Orb select(%s) yielded num_sources: %d' % (args[:1], num_sources)
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def _set_seek(self, command, *args):
        if len(args) == 0:
            seek = ORBOLDEST
        else:
            seek = int(args[0])

        self.orb.seek(seek)
        msg = 'Orb seek set to %s' % seek
        return Packet.create(msg + NEWLINE, PacketType.PA_STATUS)

    def _orb_start(self, *args):
        if self.orb_thread is None:
            self.keep_going = True
            self.orb_thread = OrbThread(self.orb, self)
            self.orb_thread.start()
            msg = 'Started orb thread'
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

    def _orb_get(self, *args):
        self.router.got_data(get_one(self.orb))

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

        packets.extend(Packet.create(pickle.dumps(d, protocol=-1), PacketType.PICKLED_FROM_INSTRUMENT))
    return packets

