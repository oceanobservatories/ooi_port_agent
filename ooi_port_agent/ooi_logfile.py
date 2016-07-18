import os
import time
from twisted.python.logfile import DailyLogFile


class ArchivingDailyLogFile(DailyLogFile):
    """A log file that is rotated daily (at or after midnight UTC)
    """
    def __init__(self, name, directory, archive_dir, defaultMode=None):
        """
        Create a log file.

        @param name: name of the file
        @param directory: directory holding the file
        @param defaultMode: permissions used to create the file. Default to
        current permissions of the file if the file exists.
        """
        self.archive_dir = archive_dir
        DailyLogFile.__init__(self, name, directory, defaultMode=defaultMode)

    def toDate(self, *args):
        """Convert a unixtime to (year, month, day) localtime tuple,
        or return the current (year, month, day) localtime tuple.

        This function primarily exists so you may overload it with
        gmtime, or some cruft to make unit testing possible.
        """
        # primarily so this can be unit tested easily
        return time.gmtime(*args)[:3]

    def suffix(self, tupledate):
        """Return the suffix given a (year, month, day) tuple or unixtime"""
        if isinstance(tupledate, (float, int, long)):
            tupledate = self.toDate(tupledate)
        if isinstance(tupledate, (tuple, list)):
            return '%d_%02d_%02d' % tupledate
        raise TypeError

    def get_archive(self):
        year, month, day = self.lastDate
        name = "%s.%s" % (self.name, self.suffix(self.lastDate))
        directory = os.path.join(self.archive_dir, str(year), '%02d' % month)
        return name, directory

    def rotate(self):
        """Rotate the file and create a new one.

        If it's not possible to open new logfile, this will fail silently,
        and continue logging to old logfile.
        """
        if not (os.access(self.directory, os.W_OK) and os.access(self.path, os.W_OK)):
            return

        newname, newdirectory = self.get_archive()
        if not os.path.exists(newdirectory):
            os.makedirs(newdirectory)

        newpath = os.path.join(newdirectory, newname)

        if not os.access(newdirectory, os.W_OK):
            return

        if os.path.exists(newpath):
            return

        self._file.close()
        os.rename(self.path, newpath)
        self._openFile()
