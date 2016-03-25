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


@click.command()
@click.option('--port', type=click.Choice(['sniff', 'command', 'data']), default='sniff')
@click.argument('refdes', nargs=1)
def connect(port, refdes):
    service_map = {
        'sniff': 'sniff-port-agent',
        'command': 'command-port-agent',
        'data': 'port-agent'
    }
    consul = consulate.Consul()
    service_id = service_map[port]
    svc = consul.health.service(service_id, tag=refdes, passing=True)
    if len(svc) == 1:
        svc = svc[0]
        addr = svc['Node']['Address']
        port = svc['Service']['Port']
        _connect(addr, port)


if __name__ == '__main__':
    connect()