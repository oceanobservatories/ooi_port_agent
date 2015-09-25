#!/usr/bin/env python

import time
import os

from twisted.internet import reactor, defer
from twisted.python import log
from smb.SMBProtocol import SMBProtocolFactory, SMBProtocol
from smb.base import SMBTimeout
from nmb.NetBIOS import NetBIOS

from agents import TcpPortAgent
from packet import Packet
from common import PacketType

# Camera will never have more than 1000 images
MAX_NUM_FILES = 1000

DOWNLOAD_TIMEOUT = 120

IMG_DIR_ROOT = '/home/asadev/camds_images/'


class CamdsPortAgent(TcpPortAgent):
    def __init__(self, config):
        super(CamdsPortAgent, self).__init__(config)

        self._start_smb_connection()

        log.msg('CamdsPortAgent initialization complete')

    def _start_smb_connection(self):

        # Credentials to login to the camera
        userID = 'guest'
        password = ''

        # client_machine_name can be an arbitary ASCII string
        # server_name should match the remote machine name, or else the connection will be rejected
        client_machine_name = 'dummy_machine'
        server_name = self.getServerName(self.inst_addr)[0]

        log.msg('Server name: ', server_name)

        download_factory = RetrieveFileFactory(userID, password, client_machine_name, server_name, use_ntlm_v2=True)

        download_factory.create_image_dir(self.name)

        reactor.connectTCP(self.inst_addr, 139, download_factory)

        reactor.run()

    def getServerName(self, remote_smb_ip, timeout=30):
        bios = NetBIOS()
        srv_name = None
        try:
            srv_name = bios.queryIPForName(remote_smb_ip, timeout=timeout)
        except:
            log.err("Couldn't find SMB server name, check remote_smb_ip again!!")
        finally:
            bios.close()
            return srv_name


class FixedProtocol(SMBProtocol):
    def _cleanupPendingRequests(self):

        if self.factory.instance == self:
            now = time.time()
            for mid, r in self.pending_requests.items():
                if r.expiry_time < now:
                    try:
                        r.errback(SMBTimeout())
                    except Exception as e:
                        log.err('Exception occurred while cleaning up pending request: ', str(e.message))

                    del self.pending_requests[mid]

            reactor.callLater(1, self._cleanupPendingRequests)


class RetrieveFileFactory(SMBProtocolFactory):
    protocol = FixedProtocol

    def __init__(self, *args, **kwargs):
        SMBProtocolFactory.__init__(self, *args, **kwargs)
        self.d = defer.Deferred()
        self.image_dir = IMG_DIR_ROOT + 'UNKNOWN_CAMDS_IMAGES'

        self.pending_file_list = []
        self.retrieved_file_list = []

    def create_image_dir(self, des):
        self.image_dir = IMG_DIR_ROOT + des + "_IMAGES"
        if not os.path.exists(self.image_dir):
            os.makedirs(self.image_dir)

    def onAuthOK(self):
        log.msg('Camds Port agent: connected successfully')

        reactor.callLater(0, self.process_existing_files)

    def onAuthFailed(self):
        self.d.errback('Auth failed')

    def process_existing_files(self):
        files_on_disk = os.listdir(self.image_dir)
        files_on_disk.sort()
        files_on_disk.reverse()

        for f in files_on_disk:

            # cleanup any unfinished downloads
            if f.endswith('.part'):
                os.remove(os.path.join(self.image_dir, f))
            elif f.endswith('.png'):
                self.retrieved_file_list.append(f)

                # we want to maintain a revolving list of the last MAX_NUM_FILES downloaded
                if len(self.retrieved_file_list) == MAX_NUM_FILES:
                    break

        log.msg('No. of existing images CAMDS agent starting out with: ', len(self.retrieved_file_list))
        reactor.callLater(0, self.list_files)

    def list_files(self):
        d = self.listPath('DCIM', '/')
        d.addCallback(self.filesListed)
        d.addErrback(self.fileListingError)

    def fetch_file(self):

        if self.pending_file_list:

            # Process the first file queued up for download
            file_name = self.pending_file_list.pop(0)

            log.msg('New Image listed, about to download: ', file_name)

            file_obj = open(self.image_dir + '/' + file_name + '.part', 'w')

            file_path = '/' + file_name

            d = self.retrieveFile('DCIM', file_path, file_obj, timeout=DOWNLOAD_TIMEOUT)
            d.addCallback(self.fileRetrieved)
            d.addErrback(self.fileRetrieveError)
        else:
            reactor.callLater(0, self.list_files)

    #Callback function when file listing is retrieved
    def filesListed(self, results):

        for img_name in results:
            file_name = img_name.filename
            if file_name.endswith('png') and (file_name not in self.retrieved_file_list)\
                    and (file_name not in self.pending_file_list):

                # Queue up pending files for download
                self.pending_file_list.append(file_name)

                # Send a message to the driver indicating that a new image has been listed
                # The driver will then associate metadata with the image file name
                packets = Packet.create('New Image:' + file_name, PacketType.FROM_INSTRUMENT)
                self.router.got_data(packets)

        reactor.callLater(0, self.fetch_file)

    #Callback function when file download is complete
    def fileRetrieved(self, write_result):

        file_obj, file_attributes, file_size = write_result

        # Remove '.part' from the end of the file name now that the download is complete
        new_file_path = file_obj.name[:-5]

        os.rename(file_obj.name, new_file_path)

        new_filename = os.path.basename(new_file_path)

        log.msg('File downloaded: ', new_filename)

        if len(self.retrieved_file_list) >= MAX_NUM_FILES:
            self.retrieved_file_list.pop(0)
        self.retrieved_file_list.append(new_filename)

        file_obj.close()

        reactor.callLater(0, self.fetch_file)

    # Error callbacks, or 'errbacks'
    def fileListingError(self, smb_timeout):
        log.msg('Error trying to list files from CAMDS share. Will attempt once again.')
        reactor.callLater(1, self.list_files)

    def fileRetrieveError(self, smb_timeout):
        log.msg('Error retrieving file from CAMDS share. Will attempt to download again.')
        reactor.callLater(1, self.list_files)
