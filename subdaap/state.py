from gevent.lock import Semaphore

import cPickle
import logging
import errno

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

        logger.debug("Saving application state to '%s'.", self.file_name)

        with self.lock:
            with open(self.file_name, "wb") as fp:
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
            except IOError as e:
                if e.errno == errno.ENOENT:
                    self.state = {}
                else:
                    raise e
            except (EOFError, cPickle.UnpicklingError):
                self.state = {}

    def __getitem__(self, key):
        """
        Proxy method for `self.state.__getitem__`.
        """
        return self.state.__getitem__(key)

    def __setitem__(self, key, value):
        """
        Proxy method for `self.state.__setitem__`.
        """
        self.state.__setitem__(key, value)

    def __contains__(self, key):
        """
        Proxy method for `self.state.__contains__`.
        """
        return self.state.__contains__(key)

    def __len__(self):
        """
        Proxy method for `self.state.__len__`.
        """
        return self.state.__len__()
