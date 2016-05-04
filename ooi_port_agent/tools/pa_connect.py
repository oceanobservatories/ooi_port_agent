#!/usr/bin/env python
import ast

import click
import consulate
import socket, select, sys

from ooi_port_agent.packet import Packet

TRIPS = '"""'


def _connect(addr, port, eol='\n', expect_packets=False):
    print 'Connecting to %s %d' % (addr, port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    s.connect((addr, port))
    buffer = ''
    print 'Connected!'

    while True:
        socket_list = [sys.stdin, s]

        read_sockets, write_sockets, error_sockets = select.select(socket_list, [], [])

        for sock in read_sockets:
            if sock == s:
                data = sock.recv(4096)
                if not data:
                    print 'Connection closed'
                    sys.exit()
                else:
                    if expect_packets:
                        buffer += data
                        while True:
                            packet, buffer = Packet.packet_from_buffer(buffer)
                            if packet:
                                print packet.logstring
                            else:
                                break
                    else:
                        print data.rstrip('\n')

            else:
                try:
                    msg = sys.stdin.readline().rstrip()
                    msg = ast.literal_eval(TRIPS + msg + TRIPS)
                    if eol:
                        msg += eol
                    s.send(msg)
                except SyntaxError:
                    print >> sys.stderr, 'Syntax Error!'


def get_host_port(service_id, tag):
    consul = consulate.Consul()
    svc = consul.health.service(service_id, tag=tag, passing=True)
    if len(svc) == 1:
        svc = svc[0]
        addr = svc['Node']['Address']
        port = svc['Service']['Port']
        return addr, port
    return None, None


@click.group()
def cli():
    pass


@cli.command()
def list():
    consul = consulate.Consul()
    services = consul.health.service('port-agent', passing=True)
    agents = []
    for svc in services:
        agents.extend(svc.get('Service', {}).get('Tags', []))
    click.echo('\n'.join(agents))


@cli.command()
@click.argument('refdes', nargs=1)
def sniff(refdes):
    addr, port = get_host_port('sniff-port-agent', refdes)
    _connect(addr, port)


@cli.command()
@click.argument('refdes', nargs=1)
def command(refdes):
    addr, port = get_host_port('command-port-agent', refdes)
    _connect(addr, port)


@cli.command()
@click.argument('refdes', nargs=1)
@click.option('--eol', default='CRLF')
def data(refdes, eol):
    eol_map = {
        'CRLF': '\r\n',
        'CR': '\r',
        'LF': '\n'
    }
    eol = eol_map.get(eol, None)

    addr, port = get_host_port('port-agent', refdes)
    _connect(addr, port, eol, expect_packets=True)


@cli.command()
@click.argument('refdes', nargs=1)
def info(refdes):
    addr, port = get_host_port('port-agent', refdes)
    addr, cmd_port = get_host_port('command-port-agent', refdes)
    addr, da_port = get_host_port('da-port-agent', refdes)
    addr, sniff_port = get_host_port('sniff-port-agent', refdes)
    print {'refdes': refdes, 'host': addr, 'data': port, 'command': cmd_port, 'sniff': sniff_port, 'da': da_port}

if __name__ == '__main__':
    cli()