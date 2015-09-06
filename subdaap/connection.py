from subdaap.subsonic import SubsonicClient
from subdaap.synchronizer import Synchronizer

import logging

# Logger instance
logger = logging.getLogger(__name__)


class Connection(object):
    """
    A connection represents a remote server and provides all the instances
    required to connect and synchronize.
    """

    def __init__(self, state, db, index, name, url, username, password,
                 synchronization, synchronization_interval, transcode,
                 transcode_unsupported):
        """
        Construct a new connection.

        :param State state: Global state object.
        :param Database db: Database object.
        :param int index: Index number that maps to a database model.
        :param str name: Name of the server and main container.
        :param str url: Remote Subsonic URL.
        :param str username: Remote Subsonic username.
        :param str password: Remote Subsonic password.
        :param str synchronization: Either 'manual', 'startup' or 'interval'.
        :param int synchronization_interval: Synchronization interval time in
                                             minutes.
        :param str transcode: Either 'all', 'unsupported' or 'no'.
        :param list transcode_unsupported: List of file extensions that are not
                                           supported, thus will be transcoded.
        """

        self.db = db
        self.state = state

        self.index = index
        self.name = name

        self.url = url
        self.username = username
        self.password = password

        self.synchronization = synchronization
        self.synchronization_interval = synchronization_interval

        self.transcode = transcode
        self.transcode_unsupported = transcode_unsupported

        self.setup_subsonic()
        self.setup_synchronizer()

    def setup_subsonic(self):
        """
        Setup a new Subsonic connection.
        """

        self.subsonic = SubsonicClient(
            url=self.url,
            username=self.username,
            password=self.password)

    def setup_synchronizer(self):
        """
        Setup a new synchronizer.
        """

        self.synchronizer = Synchronizer(
            db=self.db, state=self.state, index=self.index, name=self.name,
            subsonic=self.subsonic)

    def needs_transcoding(self, file_suffix):
        """
        Returns True if a given file suffix needs encoding, or if transcoding
        all files is set.
        """

        return self.transcode == "all" or (
            self.transcode == "unsupported" and
            file_suffix.lower() in self.transcode_unsupported)

    def get_item_fd(self, remote_id, file_suffix):
        """
        Get a file descriptor of remote connection of an item, based on
        transcoding settings.
        """

        if self.needs_transcoding(file_suffix):
            logger.debug(
                "Transcoding item '%d' with file suffix '%s'.",
                remote_id, file_suffix)
            return self.subsonic.stream(
                remote_id, tformat="mp3", estimateContentLength=True)
        else:
            return self.subsonic.download(remote_id)

    def get_artwork_fd(self, remote_id, file_suffix):
        """
        Get a file descriptor of a remote connection of an artwork item.
        """

        return self.subsonic.getCoverArt(remote_id)
