#!/usr/bin/env python
"""
Usage:
    port_agent.py --config <config_file>
    port_agent.py tcp <instaddr> <instport> <refdes> [--ttl=<ttl>]
    port_agent.py rsn <instaddr> <instport> <digiport> <refdes> [--ttl=<ttl>]
    port_agent.py botpt <instaddr> <rxport> <txport> <refdes> [--ttl=<ttl>]
    port_agent.py camds <instaddr> <instport> <imgdir> <refdes> [--ttl=<ttl>]
    port_agent.py camhd <instaddr> <subport> <reqport> <refdes> [--ttl=<ttl>]
    port_agent.py antelope <instaddr> <instport> <refdes> [--ttl=<ttl>]

Options:
    -h, --help          Show this screen.
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
from camds_agent import CamdsPortAgent


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
                    config[name] = 0
            else:
                config[name] = options[option]

    config['type'] = None
    for _type in AgentTypes.values():
        if options[_type]:
            config['type'] = _type

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
        AgentTypes.CAMDS: CamdsPortAgent,
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
