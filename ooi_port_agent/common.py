#################################################################################
# Constants
#################################################################################
import calendar
import re
import datetime
import ntplib

MAX_RECONNECT_DELAY = 240

# Interval at which router statistics are logged
ROUTER_STATS_INTERVAL = 10

# Command to set the DIGI timestamps to binary mode, sent automatically upon every DIGI connection
BINARY_TIMESTAMP = 'time 2\n'

# Interval between heartbeat packets
HEARTBEAT_INTERVAL = 10

# NEWLINE
NEWLINE = '\n'

DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


#################################################################################
# Enumerations
#################################################################################

class Enumeration(object):
    ALL = 'ALL'
    _keys = None
    _values = None
    _dict = None

    @classmethod
    def values(cls):
        """Return the values of this enum."""
        if cls._values is None:
            cls._values = tuple(getattr(cls, attr) for attr in cls.keys())
        return cls._values

    @classmethod
    def dict(cls):
        """Return a dict representation of this enum."""
        if cls._dict is None:
            cls._dict = {attr: getattr(cls, attr) for attr in cls.keys()}
        return cls._dict

    @classmethod
    def keys(cls):
        """Return the keys of this enum"""
        if cls._keys is None:
            cls._keys = tuple(attr for attr in dir(cls) if all((not callable(getattr(cls, attr)),
                                                                not attr.startswith('_'),
                                                                not attr == 'ALL')))
        return cls._keys

    @classmethod
    def has(cls, item):
        """Return if this item in the enum values"""
        return item in cls.values()

    @classmethod
    def get_key(cls, value, default=None):
        d = cls.dict()
        for key in d:
            if d[key] == value:
                return key
        return default


class AgentTypes(Enumeration):
    TCP = 'tcp'
    RSN = 'rsn'
    BOTPT = 'botpt'
    CAMDS = 'camds'
    CAMHD = 'camhd'
    ANTELOPE = 'antelope'


class Format(Enumeration):
    """Enumeration describing the possible output formats"""
    RAW = 'raw'
    PACKET = 'packet'
    ASCII = 'ascii'


class EndpointType(Enumeration):
    INSTRUMENT = 'instrument'  # TCP/RSN
    INSTRUMENT_DATA = 'instrument_data'  # BOTPT
    DIGI = 'digi_cmd'  # RSN
    CLIENT = 'client'
    COMMAND = 'command'
    LOGGER = 'logger'
    DATALOGGER = 'data_logger'
    PORT_AGENT = 'port_agent'
    COMMAND_HANDLER = 'command_handler'


class PacketType(Enumeration):
    UNKNOWN = 0
    FROM_INSTRUMENT = 1
    FROM_DRIVER = 2
    PA_COMMAND = 3
    PA_STATUS = 4
    PA_FAULT = 5
    PA_CONFIG = 6
    DIGI_CMD = 7
    DIGI_RSP = 8
    PA_HEARTBEAT = 9
    PICKLED_FROM_INSTRUMENT = 10


class RouterStat(Enumeration):
    ADD_ROUTE = 0
    ADD_CLIENT = 1
    DEL_CLIENT = 2
    PACKET_IN = 3
    PACKET_OUT = 4
    BYTES_IN = 5
    BYTES_OUT = 6


def string_to_ntp_date_time(datestr):
    """
    Extract an ntp date from a ISO8601 formatted date string.
    @param datestr an ISO8601 formatted string containing date information
    @retval an ntp date number (seconds since jan 1 1900)
    @throws InstrumentParameterException if datestr cannot be formatted to
    a date.
    """
    if not isinstance(datestr, str):
        raise IOError('Value %s is not a string.' % str(datestr))

    if not DATE_MATCHER.match(datestr):
        raise ValueError("date string not in ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSSZ")

    try:
        # This assumes input date string are in UTC (=GMT)

        # if there is no decimal place, add one to match the date format
        if datestr.find('.') == -1:
            if datestr[-1] != 'Z':
                datestr += '.0Z'
            else:
                datestr = datestr[:-1] + '.0Z'

        # if there is no trailing 'Z' on the input string add one
        if datestr[-1:] != 'Z':
            datestr += 'Z'

        dt = datetime.datetime.strptime(datestr, DATE_FORMAT)

        unix_timestamp = calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0)

        # convert to ntp (seconds since gmt jan 1 1900)
        timestamp = ntplib.system_to_ntp_time(unix_timestamp)

    except ValueError as e:
        raise ValueError('Value %s could not be formatted to a date. %s' % (str(datestr), e))

    return timestamp
