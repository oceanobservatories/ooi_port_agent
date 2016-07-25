#!/usr/bin/env python
"""
An FTP client for automatically downloading raw data files from a ZPLSC instrument.

Assumptions:
    - FTP directory listings are always in alphabetical ascending order
        - Due to file naming convention, newest/latest files are at the bottom, ie:

-rw-rw-rw-   1 owner    group       33224 Jul 13 16:30 OOI-D20150713-T155500.bot
-rw-rw-rw-   1 owner    group       38512 Jul 13 16:30 OOI-D20150713-T155500.idx
-rw-rw-rw-   1 owner    group     2882178 Jul 13 16:30 OOI-D20150713-T155500.raw
-rw-rw-rw-   1 owner    group        5768 Jul 14 07:46 OOI-D20150714-T074455.bot
-rw-rw-rw-   1 owner    group        6536 Jul 14 07:46 OOI-D20150714-T074455.idx
-rw-rw-rw-   1 owner    group      392006 Jul 14 07:46 OOI-D20150714-T074455.raw
-rw-rw-rw-   1 owner    group       10088 Jul 14 16:01 OOI-D20150714-T155459.bot
-rw-rw-rw-   1 owner    group       11520 Jul 14 16:01 OOI-D20150714-T155459.idx
-rw-rw-rw-   1 owner    group      781622 Jul 14 16:01 OOI-D20150714-T155459.raw
-rw-rw-rw-   1 owner    group        2888 Jul 14 16:05 OOI-D20150714-T160500.bot
-rw-rw-rw-   1 owner    group        3176 Jul 14 16:05 OOI-D20150714-T160500.idx
-rw-rw-rw-   1 owner    group      130526 Jul 14 16:05 OOI-D20150714-T160500.raw
-rw-rw-rw-   1 owner    group      139688 Aug 02 23:01 OOI-D20150802-T202500.bot
-rw-rw-rw-   1 owner    group      162720 Aug 02 23:01 OOI-D20150802-T202500.idx
-rw-rw-rw-   1 owner    group    12548222 Aug 02 23:01 OOI-D20150802-T202500.raw

    - FTP directory is FIFO
        - files at top of list will be deleted to make room for new files at the bottom

    - Local directory listings are always in alphabetical ascending order
        - Due to file naming convention, newest files are at the bottom/end

    - The FTP directory will not have a larger capacity than the local directory
"""

# Twisted imports
from twisted.protocols.ftp import FTPFileListProtocol
from twisted.internet import reactor, defer
from twisted.internet.protocol import connectionDone
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.ftp import FTPClient
from twisted.python import log

# Standard library imports
import fnmatch
import os
import re

from agents import PortAgent
from common import MAX_RECONNECT_DELAY
from packet import Packet
from common import PacketType

PORT = 21
RETRY = 3                       # Seconds
CHECK_INTERVAL = 60             # Seconds
SEARCH_SIZE = 10000

FILE_NAME_MATCHER = re.compile(r'.+-D(\d{4})(\d{2})(\d{2})-T\d{2}\d{2}\d{2}\.raw')


class FileWriter(Protocol):
    """
    Simple file output protocol used by FTPClient.retrieveFile() to store FTP downloaded data
    """
    def __init__(self, filename, deferred):
        self._file_name = filename
        self._file_object = None
        self._deferred = deferred

    def connectionMade(self):
        self._file_object = open(self._file_name, 'w')

    def dataReceived(self, data):
        self._file_object.write(data)

    def connectionLost(self, reason=connectionDone):
        if self._file_object:
            self._file_object.close()
        self._deferred.callback(self._file_object)


class FTPClientProtocol(FTPClient):
    def __init__(self, server_dir, local_dir, refdes, *args, **kwargs):
        FTPClient.__init__(self, *args, **kwargs)

        self._refdes = refdes
        self._server_directory = server_dir
        self._local_directory = local_dir

    def rawDataReceived(self, data):
        pass

    def connectionMade(self):
        self._change_directory()

    @defer.inlineCallbacks
    def _change_directory(self):
        """
        Change to the desired directory in the FTP server
        """
        log.msg('Changing FTP server working directory to: %s' % self._server_directory)
        try:
            yield self.cwd(self._server_directory)
            # start the download task
            self._get_files()
        except Exception as e:
            log.msg('Error changing directory in %s share: (%s)' % (self._refdes, e))
            log.msg('Retry changing working directory in %d secs' % RETRY)
            reactor.callLater(RETRY, self._change_directory)

    @defer.inlineCallbacks
    def _get_files(self):
        try:
            file_list = FTPFileListProtocol()
            yield self.list('.', file_list)
            yield self._retrieve_files(file_list)
        except Exception as e:
            log.err('Exception while listing or retrieving files (%s)' % e)

        reactor.callLater(CHECK_INTERVAL, self._get_files)

    def _should_skip(self, filename):
        # Check against files already downloaded
        return any([
            filename in self.factory.port_agent.retrieved_files,
            any([filename.endswith(ext) for ext in ['.bot', '.idx']])
        ])

    @defer.inlineCallbacks
    def _retrieve_files(self, file_list_protocol):
        """
        Queue up only new files for FTP download
        """
        for file_list_line in file_list_protocol.files:
            file_name = file_list_line['filename']
            if self._should_skip(file_name):
                continue

            # Screen for expected file names
            match = FILE_NAME_MATCHER.match(file_name)
            if not match:
                log.msg('Skipping file saved in unexpected naming format: %s' % file_name)
                continue

            # calculate the target directory, creating if necessary
            file_path = os.path.join(self._local_directory, *match.groups())
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            log.msg('New raw data file listed, about to download: ', file_name)

            part_file_name = os.path.join(file_path, self._refdes + '_' + file_name + '.part')
            writer_deferred = defer.Deferred()
            writer_deferred.addCallback(self._file_retrieved, file_list_line['size'])
            try:
                yield self.retrieveFile(file_name, FileWriter(part_file_name, writer_deferred))
            except Exception as e:
                log.err('Exception retrieving file from %s share (%s)' % (self._refdes, e))

    def _file_retrieved(self, fileobject, filesize):
        """
        Remove the temporary '.part' extension from size verified files
        Keep track of downloaded files in the retrieved file queue

        Note that this is called whether the download was successful or not,
        so we check the size to validate whether we received enough data
        """
        filename = fileobject.name
        if os.path.exists(filename):
            if filesize != os.path.getsize(filename):
                log.msg("Removing partially downloaded file: ", filename)
                os.remove(filename)
            else:
                new_filename = fileobject.name[:-5]
                os.rename(fileobject.name, new_filename)
                # Store only the original file name, no path, no reference designator
                self.factory.port_agent.retrieved_files.add(os.path.basename(new_filename).split('_')[-1])

                log.msg('File downloaded: %s (%s bytes)' % (new_filename, filesize))
                self.factory.port_agent.notify(new_filename)


class ZplscFtpClientFactory(ReconnectingClientFactory):
    """
    Factory for instrument FTP connections. Uses automatic reconnection with exponential backoff.
    """
    protocol = FTPClientProtocol
    maxDelay = MAX_RECONNECT_DELAY

    def __init__(self, port_agent, server_raw_file_dir, local_raw_file_dir, refdes, user_name, password):
        self.port_agent = port_agent
        self.server_raw_file_dir = server_raw_file_dir
        self.local_raw_file_dir = local_raw_file_dir
        self.refdes = refdes
        self.user_name = user_name
        self.password = password

    def buildProtocol(self, addr):
        log.msg('Made FTP connection to instrument (%s), building protocol' % addr)
        p = self.protocol(self.server_raw_file_dir, self.local_raw_file_dir,
                          self.refdes, self.user_name, self.password)
        p.factory = self
        self.resetDelay()
        return p


class ZplscPortAgent(PortAgent):

    def __init__(self, config):
        super(ZplscPortAgent, self).__init__(config)
        self.local_dir = config['rawdir']
        self.server_dir = config['ftpdir']
        self.username = config['user']
        self.password = config['paswd']
        self.inst_addr = config['instaddr']
        self.retrieved_files = set()
        self._startup()

    @defer.inlineCallbacks
    def _startup(self):
        try:
            yield self._process_local_files()
            self._start_inst_ftp_connection()
        except Exception as e:
            log.msg('Error STARTING (%s)' % e)
            log.msg('Retrying in %d seconds' % RETRY)
            reactor.callLater(RETRY, self._startup)

    @staticmethod
    def sleep(secs):
        """
        Deferred "sleep" to yield into the reactor
        :param secs:
        :return:
        """
        d = defer.Deferred()
        reactor.callLater(secs, d.callback, None)
        return d

    @defer.inlineCallbacks
    def _process_local_files(self):
        """
        Scan the local storage directory to remove partially downloaded files
        and add previously downloaded files to the done set.
        """
        log.msg('BEGIN inventory of local files prior to connecting to instrument')
        # recursively walk the file directory structure of the reference designator
        for path, dirs, files in os.walk(os.path.join(self.local_dir, self.refdes)):
            # Check this directory for RAW files
            # Inventory all files found
            for filename in fnmatch.filter(files, '*.raw'):
                original_filename = filename.split('_')[-1]     # remove reference designator
                self.retrieved_files.add(original_filename)
            # Check this directory for PART files
            # Remove any part files found
            for filename in fnmatch.filter(files, '*.part'):
                part_file = os.path.join(path, filename)
                log.msg("Removing partially downloaded file: %s" % part_file)
                try:
                    os.remove(part_file)
                except os.error:
                    log.msg('Unable to delete partially downloaded file: %s' % part_file)
            # Yield control briefly to allow other events to be processed
            yield self.sleep(.001)

        log.msg('COMPLETED inventory of local files prior to connecting to instrument: %d' % len(self.retrieved_files))

    # noinspection PyUnusedLocal
    def _start_inst_ftp_connection(self, *args):
        local_dir = os.path.join(self.local_dir, self.refdes)
        download_factory = ZplscFtpClientFactory(self, self.server_dir, local_dir,
                                                 self.refdes, self.username, self.password)
        reactor.connectTCP(self.inst_addr, PORT, download_factory)
        log.msg('ZplscPortAgent initialization complete')

    def client_connected(self, connection):
        """
        Log an incoming client connection
        :param connection: connection object
        :return:

        Override inherited method to so we can process any files received while there
        was no driver connected.
        """
        log.msg('CLIENT CONNECTED FROM ', connection)
        self.clients.add(connection)

        self._submit_unprocessed_files()

    def _submit_unprocessed_files(self):
        """
        Check list of local files and identify any that do not have corresponding
        echograms.  Notify driver to create echograms of these.
        :return:
        """

        log.msg('BEGIN checking list of local files on Driver connect')
        for raw_file_name in self.retrieved_files:
            # Screen for expected file names
            match = FILE_NAME_MATCHER.match(raw_file_name)
            if not match:
                log.msg('Skipping file saved in unexpected naming format: %s' % raw_file_name)
                continue

            # calculate the target directory, check it is there
            file_path = os.path.join(self.local_dir, self.refdes, *match.groups())

            if os.path.exists(file_path):
                dir_files = os.listdir(file_path)
                png_files = fnmatch.filter(dir_files, '*' + raw_file_name[:-4] + '*.png')

                if not png_files:
                    # there were no matching png files for this raw file.
                    file_name = os.path.join(file_path, self.refdes + '_' + raw_file_name)
                    self.notify(file_name)

            else:
                log.msg('No Directory corresponding to file %s. RefDes %s : ' % raw_file_name, self.refdes)

        log.msg('END checking list of local files on Driver connect')

    def notify(self, filename):
        # Send a message to the driver indicating that a new image has been retrieved
        # The driver will then associate metadata with the image file name
        packets = Packet.create('downloaded file:' + str(filename) + '\n', PacketType.FROM_INSTRUMENT)
        self.router.got_data(packets)
        log.msg('Packet sent to driver: %s' % filename)


