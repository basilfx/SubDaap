from gevent.lock import Semaphore

import cPickle
import logging

# Logger instance
logger = logging.getLogger(__name__)


class State(object):
    """
    Convenient wrapper for a state dictionary.
    """

    def __init__(self, file_name):
        """
        Construct a new State instance. The state will be directly loaded from
        file.

        :param str file_name: Path to state file.
        """

        self.file_name = file_name
        self.lock = Semaphore()
        self.state = {}

        # Unpickle state
        self.load()

    def save(self):
        """
        Save state to file.
        """

        logger.debug("Saving application state to '%s'.", self.state_file)

        with self.lock:
            with open(self.state_file, "wb") as fp:
                cPickle.dump(self.state, fp)

    def load(self):
        """
        Load state from file. If the state file is not a dictionary or if it is
        not a valid file, the state will be an empty dictionary.
        """

        logger.debug("Loading state from '%s'.", self.file_name)

        with self.lock:
            try:
                with open(self.file_name, "rb") as fp:
                    self.state = cPickle.load(fp)

                # Make sure it's a dict
                if type(self.state) != dict:
                    self.state = {}
            except (IOError, EOFError, cPickle.UnpicklingError):
                self.state = {}

    def __getitem__(self, key):
        """
        Proxy method
        """
        return self.state.__getitem__(key)

    def __setitem__(self, key, value):
        """
        Proxy method
        """
        self.state.__setitem__(key, value)

    def __contains__(self, key):
        """
        Proxy method
        """
        return self.state.__contains__(key)

    def __len__(self):
        """
        Proxy method
        """
        return self.state.__len__()
