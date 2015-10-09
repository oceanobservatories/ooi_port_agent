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
from twisted.internet import reactor, defer, task, threads
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


USER = 'ooi'                    # Default login and password
PASS = '994ef22'
PORT = 21
SEVER_DIR = 'data/Shelf_OPS'    # default raw file location in FTP server
LOCAL_DIR = '.'                 # default location to store raw data files locally
EXT = '.raw'                    # default file extension
RETRY = 3                       # Seconds

FILE_NAME_MATCHER = re.compile(r'.+-D(\d{4})(\d{2})(\d{2})-T\d{2}\d{2}\d{2}\.raw')


class FileWriter(Protocol):
    """
    Simple file output protocol used by FTPClient.retrieveFile() to store FTP downloaded data
    """
    def __init__(self, filename, deferred):
        self.file_name = filename
        self.file_object = None
        self.deferred = deferred

    def connectionMade(self):
        self.file_object = open(self.file_name, 'w')

    def dataReceived(self, data):
        self.file_object.write(data)

    def connectionLost(self, reason=connectionDone):
        if self.file_object:
            self.file_object.close()
        self.deferred.callback(self.file_object)


class FTPClientProtocol(FTPClient):

    def __init__(self, *args, **kwargs):
        FTPClient.__init__(self, *args, **kwargs)

        self.local_directory = LOCAL_DIR
        self.server_directory = SEVER_DIR

        self.search_window_size = 0
        self.old_dir_listing = []
        self.current_dir_listing = []
        # self.file_list_protocol = None

        self.retrieved_file_queue = collections.deque('')   # maintains a list of downloaded files
        self.startup = True

    def connectionMade(self):

        # ie. /home/asadev/ + ZPLSC + _IMAGES
        self.local_directory = self.factory.localDirectory + self.factory.refdes + "_IMAGES"
        self._download_process_start()

    def _download_process_start(self):
        """
        Beginning of the download process loop.
        Kick off the local file scan and listing the ftp directory.
        """

        # Defer to a thread instead of waiting for this
        deferred_local = threads.deferToThread(self._process_local_files)
        deferred_local.addErrback(self._process_local_files_errback)
        if self.startup:
            self.startup = False    # Change the directory only on startup
            deferred_server = self._change_directory()
        else:
            # This is no longer startup, go straight to listing server directory files
            deferred_server = self._list_files()

        # Don't continue until both tasks finish, then retrieve the files from the server listing
        defer.DeferredList([deferred_server, deferred_local]).addCallback(
            lambda result: task.coiterate(self._retrieve_files(result)))

    def _process_local_files(self):
        """
        Scan the local storage directory to remove partially downloaded files
        and add previously downloaded files to the done queue.
        """

        # recursively walk the image file directory structure
        for root, dirnames, filenames in os.walk(self.local_directory):
            for filename in fnmatch.filter(filenames, '*.raw'):
                original_filename = filename.split('_')[-1]     # remove reference designator
                if original_filename not in self.retrieved_file_queue:
                    self.retrieved_file_queue.append(original_filename)
            for basename in fnmatch.filter(filenames, '*.part'):
                part_file = os.path.join(root, basename)
                log.msg("Removing partially downloaded file: ", part_file)
                os.remove(part_file)

        log.msg('Number of existing data files on local disk (%s agent): ' % self.factory.refdes,
                len(self.retrieved_file_queue))

    def _process_local_files_errback(self, fail_msg):
        log.msg('Error processing files in directory: %s:' % self.local_directory)
        log.err(str(fail_msg))
        log.msg('Retry processing files...')
        d = task.deferLater(reactor, RETRY, self._process_local_files)
        d.addErrback(self._process_local_files_errback)
        return d

    def _change_directory(self):
        """
        Change to the desired directory in the FTP server
        """
        log.msg('Changing FTP server working directory to: %s' % self.server_directory)
        d = self.cwd(self.server_directory).addCallback(lambda ignore: self._list_files())
        d.addErrback(self._change_directory_errback)
        return d

    def _change_directory_errback(self, fail_msg):
        log.msg('Error changing directory in %s share:' % self.factory.refdes)
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
        d.addCallback(lambda ignore: defer.succeed(file_list))  # pass only file_list forward
        d.addErrback(self._list_files_errback)
        return d

    def _list_files_errback(self, fail_msg):

        log.msg('Error listing files from %s share:' % self.factory.refdes)
        log.err(str(fail_msg))
        log.msg('Retry file list...')
        d = task.deferLater(reactor, RETRY, self._list_files).addErrback(self._list_files_errback)
        return d

    def _retrieve_files(self, result):
        """
        Queue up only new files for FTP download
        """
        # Get the populated file list protocol from the deferred list results
        (server_result, file_list_protocol), ignore_local_result = result[:]

        d = None

        for file_list_line in file_list_protocol.files:
            file_name = file_list_line['filename']

            # Ignore .bot & .idx files
            if any([file_name.endswith(ext) for ext in ['.bot', '.idx']]):
                continue

            # Screen for expected file names
            match = FILE_NAME_MATCHER.match(file_name)
            if not match:
                log.err('Skipping image saved in unexpected file format: %s' % file_name)
                continue

            # Check against files already downloaded
            if file_name in self.retrieved_file_queue:
                continue

            file_path = os.path.join(self.local_directory, *match.groups())
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            log.msg('New raw data listed, about to download: ', file_name)

            path_file_name = os.path.join(file_path, self.factory.refdes + '_' + file_name + '.part')
            dreturn = defer.Deferred().addCallback(self._file_retrieved, file_list_line['size'])
            d = self.retrieveFile(file_name, FileWriter(path_file_name, dreturn))
            d.addErrback(self._retrieve_files_errback)

            yield d     # wait for this, but go ahead & do something else

        if d:
            # With single threading, the for loop above occurs in order
            # Add a callback to the last deferred to loop back to start immediately
            # Note: if we thread this out use a DeferredList
            d.addBoth(lambda ignore: self._download_process_start())
        else:
            # No new files to download yet, try again later
            reactor.callLater(5, self._download_process_start)
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
            self.retrieved_file_queue.append(os.path.basename(new_filename).split('_')[-1])

            log.msg('File downloaded: %s (%s bytes)' % (new_filename, filesize))

    def _retrieve_files_errback(self, fail_msg):
        log.msg('Error retrieving file from %s share:' % self.factory.refdes)
        log.err(str(fail_msg))
        # Just log the error and let it loop back to start
        log.msg('Retry file download in next iteration')


class ZplscFtpClientFactory(ReconnectingClientFactory):
    """
    Factory for instrument FTP connections. Uses automatic reconnection with exponential backoff.
    """
    protocol = FTPClientProtocol
    maxDelay = MAX_RECONNECT_DELAY

    def __init__(self, user_name, password):
        self.user_name = user_name
        self.password = password

    def buildProtocol(self, addr):
        log.msg('Made FTP connection to instrument (%s), building protocol' % addr)
        p = self.protocol(self.user_name, self.password)
        p.factory = self
        self.resetDelay()
        return p


class ZplscPortAgent(TcpPortAgent):

    def __init__(self, config):
        super(ZplscPortAgent, self).__init__(config)
        self.local_dir = config['imgdir']
        self.username = config.get('user', USER)
        self.password = config.get('paswd', PASS)
        self._start_inst_ftp_connection()
        log.msg('ZplscPortAgent initialization complete')

    def _start_inst_ftp_connection(self):

        download_factory = ZplscFtpClientFactory(self.username, self.password)
        download_factory.localDirectory = self.local_dir
        download_factory.refdes = self.refdes
        reactor.connectTCP(self.inst_addr, PORT, download_factory)