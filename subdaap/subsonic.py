import urlparse
import libsonic
import logging

# Logger instance
logger = logging.getLogger(__name__)

class Connection(libsonic.Connection):
    """
    Wraps `libsonic.Connection` to fix a few issues.

    - Parse URL for host and port.
    - Add name propertie
    - Make sure API results are of correct type
    """

    def __init__(self, name, url, username, password):
        self.name = name

        # Parse SubSonic URL
        parts = urlparse.urlparse(url)
        scheme = parts.scheme or "http"

        # Make sure there is hostname
        if not parts.hostname:
            raise ValueError("Expected hostname for URL: %s" % url)

        # Validate scheme
        if scheme not in ("http", "https"):
            raise ValueError("Unexpected scheme '%s' for URL: %s" % (
                scheme, url))

        # Pick a default port
        host = "%s://%s" % (scheme, parts.hostname)
        port = parts.port or {"http": 80, "https": 443}[scheme]

        # Invoke original constructor
        super(Connection, self).__init__(host, username, password, port=port)