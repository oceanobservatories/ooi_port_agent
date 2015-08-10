#!/usr/bin/env python
"""
Usage:
    port_agent.py --config <config_file>
    port_agent.py tcp <instaddr> <instport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py tcp <port> <commandport> <instaddr> <instport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py rsn <instaddr> <instport> <digiport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py rsn <port> <commandport> <instaddr> <instport> <digiport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py botpt <instaddr> <rxport> <txport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py botpt <port> <commandport> <instaddr> <rxport> <txport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py camhd <instaddr> <subport> <reqport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py camhd <port> <commandport> <instaddr> <subport> <reqport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py antelope <instaddr> <instport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py antelope <port> <commandport> <instaddr> <instport> [--sniff=<sniffport>] [--name=<name>] [--refdes=<refdes>] [--ttl=<ttl>]
    port_agent.py datalog <files>...
    port_agent.py datalog <port> <commandport> <files>...
    port_agent.py digilog_ascii <files>...
    port_agent.py digilog_ascii <port> <commandport> <files>...
    port_agent.py chunky <files>...
    port_agent.py chunky <port> <commandport> <files>...

Options:
    -h, --help          Show this screen.
    --sniff=<sniffport> Start a sniffer on this port
    --name=<name>       Name this port agent (for logfiles, otherwise commandport is used)
    --refdes=<refdes>   Reference designator for this port agent (for consul local service ID, otherwise type is used)
    --ttl=<ttl>         The TTL Check status interval of consul local service

"""
import logging
import os

from docopt import docopt
from twisted.internet import reactor
from twisted.python import log
import yaml

from common import AgentTypes
from agents import TcpPortAgent
from agents import RsnPortAgent
from agents import BotptPortAgent
from agents import DatalogReadingPortAgent
from agents import DigiDatalogAsciiPortAgent
from agents import ChunkyDatalogPortAgent


def configure_logging():
    log_format = '%(asctime)-15s %(levelname)s %(message)s'
    logging.basicConfig(format=log_format)
    logger = logging.getLogger('port_agent')
    logger.setLevel(logging.INFO)
    observer = log.PythonLoggingObserver('port_agent')
    observer.start()


def config_from_options(options):
    if options['--config']:
        return yaml.load(open(options['--config']))

    config = {}
    for option in options:
        if option.startswith('<'):
            name = option[1:-1]
            if 'port' in name:
                try:
                    config[name] = int(options[option])
                except (ValueError, TypeError):
                    config[name] = 0    # default to a random port
            else:
                config[name] = options[option]

    config['type'] = None
    for _type in AgentTypes.values():
        if options[_type]:
            config['type'] = _type

    sniff = options['--sniff']
    if sniff is not None:
        try:
            sniff = int(sniff)
        except (ValueError, TypeError):
            sniff = None
        config['sniffport'] = sniff
    else:
        config['sniffport'] = 0

    name = options['--name']
    if name is not None:
        config['name'] = name

    refdes = options['--refdes']
    if refdes is not None:
        config['refdes'] = refdes

    ttl = options['--ttl']
    if ttl is not None:
        try:
            ttl = int(ttl)
        except (ValueError, TypeError):
            ttl = None
        config['ttl'] = ttl
    else:
        config['ttl'] = 30

    return config


def main():
    configure_logging()
    options = docopt(__doc__)
    config = config_from_options(options)

    try:
        from camhd_agent import CamhdPortAgent
    except ImportError:
        CamhdPortAgent = None
        log.err('Unable to import CAMHD libraries, CAMHD port agent unavailable')

    try:
        os.environ['ANTELOPE_PYTHON_GILRELEASE'] = '1'
        from antelope_agent import AntelopePortAgent
    except ImportError:
        AntelopePortAgent = None
        log.err('Unable to import Antelope libraries, Antelope port agent unavailable')

    agent_type_map = {
        AgentTypes.TCP: TcpPortAgent,
        AgentTypes.RSN: RsnPortAgent,
        AgentTypes.BOTPT: BotptPortAgent,
        AgentTypes.DATALOG: DatalogReadingPortAgent,
        AgentTypes.DIGILOG_ASCII: DigiDatalogAsciiPortAgent,
        AgentTypes.CHUNKY: ChunkyDatalogPortAgent,
        AgentTypes.CAMHD: CamhdPortAgent,
        AgentTypes.ANTELOPE: AntelopePortAgent
    }

    agent_type = config['type']
    agent = agent_type_map.get(agent_type)

    if agent is not None:
        agent(config)
        exit(reactor.run())
    else:
        exit(1)

if __name__ == '__main__':
    main()
