from subdaap import utils

import urlparse
import libsonic


class Connection(libsonic.Connection):
    """
    Extend `libsonic.Connection` with new features and fix a few issues.

    - Parse URL for host and port for constructor.
    - Make sure API results are of of uniform type.
    - Add transcoding options for internal use

    :param str name: Identifiable name that represents this connection.
    :param str url: Full URL (including scheme) of the SubSonic server.
    :param str username: Username of the server.
    :param str password: Password of the server.
    :param str transcode: Either 'all', 'unsupported' or 'no'.
    :param list transcode_unsupported: List of file extensions that are not
                                       supported, thus will be transcoded.
    """

    def __init__(self, name, url, username, password, transcode,
                 transcode_unsupported):
        """
        """

        # Save some connection related settings.
        self.name = name
        self.transcode = transcode
        self.transcode_unsupported = transcode_unsupported

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

    def getIndexes(self, *args, **kwargs):
        """
        """

        def _artists_iterator(artists):
            for artist in utils.force_list(artists):
                artist["id"] = int(artist["id"])
                yield artist

        def _index_iterator(index):
            for index in utils.force_list(index):
                index["artist"] = list(_artists_iterator(index.get("artist")))
                yield index

        def _children_iterator(children):
            for child in utils.force_list(children):
                child["id"] = int(child["id"])

                if "parent" in child:
                    child["parent"] = int(child["parent"])
                if "coverArt" in child:
                    child["coverArt"] = int(child["coverArt"])
                if "artistId" in child:
                    child["artistId"] = int(child["artistId"])
                if "albumId" in child:
                    child["albumId"] = int(child["albumId"])

                yield child

        response = super(Connection, self).getIndexes(*args, **kwargs)
        response["indexes"] = response.get("indexes", {})
        response["indexes"]["index"] = list(
            _index_iterator(response["indexes"].get("index")))
        response["indexes"]["child"] = list(
            _children_iterator(response["indexes"].get("child")))

        return response

    def getPlaylists(self, *args, **kwargs):
        """
        """

        def _playlists_iterator(playlists):
            for playlist in utils.force_list(playlists):
                playlist["id"] = int(playlist["id"])
                yield playlist

        response = super(Connection, self).getPlaylists(*args, **kwargs)
        response["playlists"]["playlist"] = list(
            _playlists_iterator(response["playlists"].get("playlist")))

        return response

    def getPlaylist(self, *args, **kwargs):
        """
        """

        def _entries_iterator(entries):
            for entry in utils.force_list(entries):
                entry["id"] = int(entry["id"])
                yield entry

        response = super(Connection, self).getPlaylist(*args, **kwargs)
        response["playlist"]["entry"] = list(
            _entries_iterator(response["playlist"].get("entry")))

        return response

    def getArtist(self, *args, **kwargs):
        """
        """

        def _albums_iterator(albums):
            for album in utils.force_list(albums):
                album["id"] = int(album["id"])

                if "artistId" in album:
                    album["artistId"] = int(album["artistId"])

                yield album

        response = super(Connection, self).getArtist(*args, **kwargs)
        response["artist"]["album"] = list(
            _albums_iterator(response["artist"].get("album")))

        return response

    def getMusicDirectory(self, *args, **kwargs):
        """
        """

        def _children_iterator(children):
            for child in utils.force_list(children):
                child["id"] = int(child["id"])

                if "parent" in child:
                    child["parent"] = int(child["parent"])
                if "coverArt" in child:
                    child["coverArt"] = int(child["coverArt"])
                if "artistId" in child:
                    child["artistId"] = int(child["artistId"])
                if "albumId" in child:
                    child["albumId"] = int(child["albumId"])

                yield child

        response = super(Connection, self).getMusicDirectory(*args, **kwargs)
        response["directory"]["child"] = list(
            _children_iterator(response["directory"].get("child")))

        return response
