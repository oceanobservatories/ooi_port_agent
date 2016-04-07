#!/usr/bin/env python

import click
import consulate
import socket, select, sys


def _connect(addr, port):
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
                s.send(msg)


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


service_map = {
    'sniff': 'sniff-port-agent',
    'command': 'command-port-agent',
    'data': 'port-agent'
}


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
def data(refdes):
    addr, port = get_host_port('port-agent', refdes)
    _connect(addr, port)


if __name__ == '__main__':
    cli()