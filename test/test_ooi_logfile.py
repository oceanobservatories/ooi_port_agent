import unittest
from io import StringIO

import os
from mock import mock
from ooi_port_agent.ooi_logfile import ArchivingDailyLogFile


def mock_openfile(self):
    self.lastDate = (2016, 1, 1)
    return StringIO()


def mock_init(self, name, directory, archive_dir, defaultMode=None):
    """
    Create a log file.

    @param name: name of the file
    @param directory: directory holding the file
    @param defaultMode: permissions used to create the file. Default to
    current permissions of the file if the file exists.
    """
    self.directory = directory
    self.name = name
    self.path = os.path.join(directory, name)
    self.archive_dir = archive_dir
    mock_openfile(self)


@mock.patch('ooi_port_agent.ooi_logfile.ArchivingDailyLogFile.__init__', new=mock_init)
class LogfileUnitTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_should_rotate(self):
        logfile = ArchivingDailyLogFile('', '', '')
        self.assertTrue(logfile.shouldRotate())

        logfile.lastDate = logfile.toDate()
        self.assertFalse(logfile.shouldRotate())

    def test_get_archive(self):
        fname = 'test'
        logdir = 'logdir'
        archivedir = 'archivedir'
        logfile = ArchivingDailyLogFile(fname, logdir, archivedir)
        newname, newdir = logfile.get_archive()

        self.assertEqual(newname, fname + '.2016_01_01')
        self.assertEqual(newdir, os.path.join(archivedir, '2016', '01'))

    def test_suffix(self):
        logfile = ArchivingDailyLogFile('', '', '')
        self.assertEqual('2016_01_01', logfile.suffix((2016, 1, 1)))
        self.assertEqual('2016_12_31', logfile.suffix((2016, 12, 31)))
        self.assertEqual('1970_01_01', logfile.suffix(0))

