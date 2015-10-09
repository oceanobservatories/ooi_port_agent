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
from twisted.internet import reactor, defer, task
from twisted.internet.protocol import connectionDone
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.ftp import FTPClient
from twisted.python import log

# Standard library imports
import collections
import fnmatch
import os
import re

from agents import TcpPortAgent
from common import MAX_RECONNECT_DELAY


PORT = 21
SERVER_DIR = 'data/Shelf_OPS'   # default raw file location in FTP server
LOCAL_DIR = '.'                 # default location to store raw data files locally
EXT = '.raw'                    # default file extension
RETRY = 3                       # Seconds
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

    def __init__(self, local_dir, refdes, *args, **kwargs):
        FTPClient.__init__(self, *args, **kwargs)

        self._refdes = refdes
        self._local_directory = local_dir
        self._server_directory = SERVER_DIR

    def connectionMade(self):

        if not self.factory.retrieved_file_queue:
            # Create once, queue persists in the factory
            self.factory.retrieved_file_queue = collections.deque('', SEARCH_SIZE)

        # Only need to scan the local directory during startup
        if len(self.factory.retrieved_file_queue) == 0:
            d = defer.execute(self._process_local_files)
            d.addErrback(self._process_local_files_errback)
            d.addCallback(lambda ignore: self._change_directory())    # suppress the returned result
        else:
            self._change_directory()

    def _process_local_files(self):
        """
        Scan the local storage directory to remove partially downloaded files
        and add previously downloaded files to the done queue.
        """

        # recursively walk the file directory structure
        for root, dirnames, filenames in os.walk(self._local_directory):
            for filename in fnmatch.filter(filenames, '*.raw'):
                original_filename = filename.split('_')[-1]     # remove reference designator
                if original_filename not in self.factory.retrieved_file_queue:
                    self.factory.retrieved_file_queue.append(original_filename)
            for basename in fnmatch.filter(filenames, '*.part'):
                part_file = os.path.join(root, basename)
                log.msg("Removing partially downloaded file: ", part_file)
                os.remove(part_file)

        log.msg('Number of existing data files on local disk (%s agent): ' % self._refdes,
                len(self.factory.retrieved_file_queue))

    def _process_local_files_errback(self, fail_msg):
        log.msg('Error processing files in directory: %s:' % self._local_directory)
        log.err(str(fail_msg))
        log.msg('Retry processing files...')
        d = task.deferLater(reactor, RETRY, self._process_local_files)
        d.addErrback(self._process_local_files_errback)
        return d

    def _change_directory(self):
        """
        Change to the desired directory in the FTP server
        """
        log.msg('Changing FTP server working directory to: %s' % self._server_directory)
        d = self.cwd(self._server_directory).addCallback(lambda ignore: self._list_files())
        d.addErrback(self._change_directory_errback)
        return d

    def _change_directory_errback(self, fail_msg):
        log.msg('Error changing directory in %s share:' % self._refdes)
        log.err(str(fail_msg))
        log.msg('Retry changing working directory...')
        d = task.deferLater(reactor, RETRY, self._change_directory)
        d.addErrback(self._change_directory_errback)
        return d

    def _list_files(self):
        """
        Get a detailed listing of the current directory
        """

        file_list = FTPFileListProtocol()
        d = self.list('.', file_list)
        d.addCallback(lambda ignore: task.coiterate(self._retrieve_files(file_list)))
        d.addErrback(self._list_files_errback)
        return d

    def _list_files_errback(self, fail_msg):

        log.msg('Error listing files from %s share:' % self._refdes)
        log.err(str(fail_msg))
        log.msg('Retry file list...')
        d = task.deferLater(reactor, RETRY, self._list_files).addErrback(self._list_files_errback)
        return d

    def _retrieve_files(self, file_list_protocol):
        """
        Queue up only new files for FTP download
        """

        d = None

        for file_list_line in file_list_protocol.files:
            file_name = file_list_line['filename']

            # Ignore .bot & .idx files
            if any([file_name.endswith(ext) for ext in ['.bot', '.idx']]):
                continue

            # Screen for expected file names
            match = FILE_NAME_MATCHER.match(file_name)
            if not match:
                log.err('Skipping file saved in unexpected naming format: %s' % file_name)
                continue

            # Check against files already downloaded
            if file_name in self.factory.retrieved_file_queue:
                continue

            file_path = os.path.join(self._local_directory, *match.groups())
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            log.msg('New raw data file listed, about to download: ', file_name)

            path_file_name = os.path.join(file_path, self._refdes + '_' + file_name + '.part')
            dreturn = defer.Deferred().addCallback(self._file_retrieved, file_list_line['size'])
            d = self.retrieveFile(file_name, FileWriter(path_file_name, dreturn))
            d.addErrback(self._retrieve_files_errback, path_file_name)

            yield d     # wait for this, but go ahead & do something else

        if d:
            # With single threading, the for loop above occurs in order
            # Add a callback to the last deferred to loop back to start immediately
            # Note: if we thread this out use a DeferredList
            d.addBoth(lambda ignore: self._list_files())        # suppress the returned result
        else:
            # No new files to download yet, try again later
            reactor.callLater(5, self._list_files)
            # TODO: rand-exp-backoff <= max_ftp_timeout (keep connection alive)?

    def _file_retrieved(self, fileobject, filesize):
        """
        Remove the temporary '.part' extension from size verified files
        Keep track of downloaded files in the retrieved file queue
        """

        if filesize == os.path.getsize(fileobject.name):
            new_filename = fileobject.name[:-5]
            os.rename(fileobject.name, new_filename)
            # Store only the original file name, no path, no reference designator
            self.factory.retrieved_file_queue.append(os.path.basename(new_filename).split('_')[-1])

            log.msg('File downloaded: %s (%s bytes)' % (new_filename, filesize))

    def _retrieve_files_errback(self, fail_msg, filename):
        log.msg('Error retrieving file from %s share:' % self._refdes)
        log.err(str(fail_msg))
        log.msg("Removing partially downloaded file: ", filename)
        os.remove(filename)
        # Just log the error and let it loop back to start
        log.msg('Retry file download in next iteration')


class ZplscFtpClientFactory(ReconnectingClientFactory):
    """
    Factory for instrument FTP connections. Uses automatic reconnection with exponential backoff.
    """
    protocol = FTPClientProtocol
    maxDelay = MAX_RECONNECT_DELAY
    retrieved_file_queue = None     # maintains a list of downloaded files

    def __init__(self, local_raw_file_dir, refdes, user_name, password):
        self.local_raw_file_dir = local_raw_file_dir
        self.refdes = refdes
        self.user_name = user_name
        self.password = password

    def buildProtocol(self, addr):
        log.msg('Made FTP connection to instrument (%s), building protocol' % addr)
        p = self.protocol(self.local_raw_file_dir, self.refdes, self.user_name, self.password)
        p.factory = self
        self.resetDelay()
        return p


class ZplscPortAgent(TcpPortAgent):

    def __init__(self, config):
        super(ZplscPortAgent, self).__init__(config)
        self.local_dir = config['rawdir']
        self.username = config['user']
        self.password = config['paswd']
        self._start_inst_ftp_connection()
        log.msg('ZplscPortAgent initialization complete')

    def _start_inst_ftp_connection(self):

        local_raw_file_dir = os.path.join(self.local_dir, self.refdes)
        download_factory = ZplscFtpClientFactory(
            local_raw_file_dir, self.refdes, self.username, self.password)
        reactor.connectTCP(self.inst_addr, PORT, download_factory)