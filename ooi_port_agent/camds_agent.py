#!/usr/bin/env python

import time
import os
import re
import collections
import fnmatch

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

IMG_REGEX = '.*(\d{4})(\d{2})(\d{2})T.*jpg'
IMG_PATTERN = re.compile(IMG_REGEX, re.DOTALL)


class CamdsPortAgent(TcpPortAgent):

	img_dir_root = IMG_DIR_ROOT

	def __init__(self, config):
		super(CamdsPortAgent, self).__init__(config)

		self.img_dir_root = config['imgdir']

		self._start_smb_connection()

		log.msg('CamdsPortAgent initialization complete')

	def my_callback(self):
		pass

	def _start_smb_connection(self):

		# Credentials to login to the camera
		userID = 'subc'
		password = 'password'
		share_name = 'Media'

		# client_machine_name can be an arbitary ASCII string
		# server_name should match the remote machine name, or else the connection will be rejected
		client_machine_name = 'dummy_machine'
		server_name = self.getServerName(self.inst_addr)[0]

		log.msg('Server name: ', server_name)

		download_factory = RetrieveFileFactory(self.inst_addr, self.router, self.refdes, self.img_dir_root, share_name,
											   userID, password, client_machine_name, server_name, use_ntlm_v2=True)

		reactor.connectTCP(host=self.inst_addr, port=139, factory=download_factory)

	def getServerName(self, remote_smb_ip, timeout=30):
		bios = NetBIOS()
		srv_name = None
		try:
			srv_name = bios.queryIPForName(ip=remote_smb_ip, timeout=timeout)
		except:
			log.err("Couldn't find SMB server name, check remote_smb_ip again!!")
		finally:
			bios.close()
			return srv_name

# This change can be removed once Pete's fix has been published upstream
# Original code was using iteritems() vs. items, which was causing an exception.
# We wouldn't then need to override this method
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

	def __init__(self, inst_addr, router, ref_des, img_dir_root, share_name, *args, **kwargs):
		SMBProtocolFactory.__init__(self, *args, **kwargs)
		self.d = defer.Deferred()
		self.instrument_ip = inst_addr
		self.router = router
		self.ref_des = ref_des
		self.share_name = share_name

		self.img_dir_root = img_dir_root
		self.image_dir = self.img_dir_root + self.ref_des
		if not os.path.exists(self.image_dir):
			os.makedirs(self.image_dir)

		self.pending_file_queue = collections.deque('')
		self.retrieved_file_queue = collections.deque('', MAX_NUM_FILES)

	def onAuthOK(self):
		log.msg('Camds Port agent: connected successfully')

		reactor.callLater(0, self.process_existing_files)

	def onAuthFailed(self):
		log.msg('Authentication failed - attempting to reconnect...')

		self.instance.transport.loseConnection()

		# wait for a little bit before trying to reconnect
		reactor.callLater(5, self.reconnect)

	def reconnect(self):
		# try to reconnect
		download_factory = RetrieveFileFactory(self.instrument_ip, self.router, self.ref_des, self.img_dir_root, self.share_name,
											   self.username, self.password, self.my_name, self.remote_name, use_ntlm_v2=True)

		reactor.connectTCP(self.instrument_ip, 139, download_factory)

	def process_existing_files(self):

		files_on_disk = []

		# recursively walk the image file directory structure
		for root, dirnames, filenames in os.walk(self.image_dir):
			for filename in fnmatch.filter(filenames, '*.jpg'):
				files_on_disk.append(filename)
			for basename in fnmatch.filter(filenames, '*.part'):
				part_file = os.path.join(root, basename)
				log.msg("cleaning up partially downloaded file: ", part_file)
				os.remove(part_file)

		# we want to store only the latest image file names in the queue
		files_on_disk.sort()

		for f in files_on_disk:
			fname = f.split('_')[1]
			self.retrieved_file_queue.append(fname)

		log.msg('Number of existing images CAMDS agent starting out with: ', len(self.retrieved_file_queue))
		reactor.callLater(0, self.list_dirs)
		
	def list_dirs(self):
		d = self.listPath(service_name=self.share_name, path='//', pattern='Stills*')
		d.addCallback(self.dirsListed)
		d.addErrback(self.dirListingError)

	def list_files(self, fpath):
		d = self.listPath(service_name=self.share_name, path=fpath, pattern='*.jpg')
		d.addCallback(self.filesListed, fpath)
		d.addErrback(self.fileListingError, fpath)

	def fetch_file(self):
		if self.pending_file_queue:

			# Process the first file queued up for download
			file_name = self.pending_file_queue.popleft()

			log.msg('New Image listed, about to download: ', file_name)

			match = IMG_PATTERN.match(file_name)

			if not match:
				log.err('Skipping image saved in unexpected file format: ', file_name)
				reactor.callLater(0, self.list_dirs)

			year = match.group(1)
			month = match.group(2)
			day = match.group(3)

			image_path = os.path.join(self.image_dir, year, month, day)

			if not os.path.exists(image_path):
				os.makedirs(image_path)

			dest_file_name = self.ref_des + '_' + os.path.basename(file_name)
			file_obj = open(os.path.join(image_path, dest_file_name) + '.part', 'w')

			file_path = '/' + file_name

			d = self.retrieveFile(service_name=self.share_name, path=file_path, file_obj=file_obj, timeout=DOWNLOAD_TIMEOUT)
			d.addCallback(self.fileRetrieved)
			d.addErrback(self.fileRetrieveError)
		else:
			reactor.callLater(0, self.list_dirs)

	#Callback function for when directory listing is receieved
	def dirsListed(self, results):
		log.msg('Found', len(results), 'directories')
		for dir_name in results:
			dname = dir_name.filename
			if dname == '.' or dname == '..':
				continue
			log.msg('Found:', dname)
			
			if all([dname.startswith('Stills'), '/' not in dname]):
				log.msg('Decending into:', dname)
				reactor.callLater(0, self.list_files, fpath=dname.strip())

		reactor.callLater(0, self.fetch_file)

	#Callback function when file listing is retrieved
	def filesListed(self, results, fpath):
		log.msg('Found', len(results), 'images in', fpath)
		for img_name in results:
			file_name = os.path.join(fpath, img_name.filename)

			if all([
					file_name.endswith('jpg'),
					file_name not in self.retrieved_file_queue,
					file_name not in self.pending_file_queue
			]):
				# Queue up pending files for download
				log.msg('Number of files to be downloaded:', len(self.pending_file_queue))
				self.pending_file_queue.append(file_name)

	#Callback function when file download is complete
	def fileRetrieved(self, write_result):

		file_obj, file_attributes, file_size = write_result

		# Remove '.part' from the end of the file name now that the download is complete
		new_file_path = file_obj.name[:-5]

		os.rename(file_obj.name, new_file_path)

		new_filename = os.path.basename(new_file_path)

		log.msg('File downloaded: ', new_filename)

		orig_filename = new_filename.split('_')[1]

		self.retrieved_file_queue.append(orig_filename)

		file_obj.close()

		# Send a message to the driver indicating that a new image has been retrieved
		# The driver will then associate metadata with the image file name
		packets = Packet.create('New Image:' + str(new_filename), PacketType.FROM_INSTRUMENT)
		self.router.got_data(packets)

		reactor.callLater(0, self.fetch_file)

	# Error callbacks, or 'errbacks'
	def dirListingError(self, fail_msg):
		log.msg('Error trying to list directories from CAMDS share:')
		log.err(str(fail_msg))
		log.msg('Will attempt to list directories once again...')
		reactor.callLater(1, self.list_dirs)
	
	def fileListingError(self, fail_msg, fpath):
		log.msg('Error trying to list files from CAMDS share path:', fpath)
		log.err(str(fail_msg))
		log.msg('Will attempt to list files once again...')
		reactor.callLater(1, self.list_files, fpath=fpath)

	def fileRetrieveError(self, fail_msg):
		log.msg('Error retrieving file from CAMDS share:')
		log.err(str(fail_msg))
		log.msg('Will attempt to download again...')
		reactor.callLater(1, self.list_dirs)
