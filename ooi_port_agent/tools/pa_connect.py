#!/usr/bin/env python

import click
import consulate
import socket, select, sys


def _connect(addr, port, eol='\n'):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    s.connect((addr, port))

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
                    sys.stdout.write(data)

            else:
                msg = sys.stdin.readline()
                s.send(msg.rstrip() + eol)


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
    eol = eol_map.get(eol, eol_map['CRLF'])

    addr, port = get_host_port('port-agent', refdes)
    _connect(addr, port, eol)


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