from subdaap import utils

import urlparse
import libsonic


class SubsonicClient(libsonic.Connection):
    """
    Extend `libsonic.Connection` with new features and fix a few issues.

    - Parse URL for host and port for constructor.
    - Make sure API results are of of uniform type.
    - Add transcoding options for internal use
    """

    def __init__(self, url, username, password):
        """
        Construct a new SubsonicClient.

        :param str url: Full URL (including scheme) of the SubSonic server.
        :param str username: Username of the server.
        :param str password: Password of the server.
        """

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
        super(SubsonicClient, self).__init__(
            host, username, password, port=port)

    def getIndexes(self, *args, **kwargs):
        """
        Improve the getIndexes method. Ensures IDs are integers.
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

        response = super(SubsonicClient, self).getIndexes(*args, **kwargs)
        response["indexes"] = response.get("indexes", {})
        response["indexes"]["index"] = list(
            _index_iterator(response["indexes"].get("index")))
        response["indexes"]["child"] = list(
            _children_iterator(response["indexes"].get("child")))

        return response

    def getPlaylists(self, *args, **kwargs):
        """
        Improve the getPlaylists method. Ensures IDs are integers.
        """

        def _playlists_iterator(playlists):
            for playlist in utils.force_list(playlists):
                playlist["id"] = int(playlist["id"])
                yield playlist

        response = super(SubsonicClient, self).getPlaylists(*args, **kwargs)
        response["playlists"]["playlist"] = list(
            _playlists_iterator(response["playlists"].get("playlist")))

        return response

    def getPlaylist(self, *args, **kwargs):
        """
        Improve the getPlaylist method. Ensures IDs are integers.
        """

        def _entries_iterator(entries):
            for entry in utils.force_list(entries):
                entry["id"] = int(entry["id"])
                yield entry

        response = super(SubsonicClient, self).getPlaylist(*args, **kwargs)
        response["playlist"]["entry"] = list(
            _entries_iterator(response["playlist"].get("entry")))

        return response

    def getArtist(self, *args, **kwargs):
        """
        Improve the getArtist method. Ensures IDs are integers.
        """

        def _albums_iterator(albums):
            for album in utils.force_list(albums):
                album["id"] = int(album["id"])

                if "artistId" in album:
                    album["artistId"] = int(album["artistId"])

                yield album

        response = super(SubsonicClient, self).getArtist(*args, **kwargs)
        response["artist"]["album"] = list(
            _albums_iterator(response["artist"].get("album")))

        return response

    def getMusicDirectory(self, *args, **kwargs):
        """
        Improve the getMusicDirectory method. Ensures IDs are integers.
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

        response = super(SubsonicClient, self).getMusicDirectory(
            *args, **kwargs)
        response["directory"]["child"] = list(
            _children_iterator(response["directory"].get("child")))

        return response

    def walk_index(self):
        """
        Request SubSonic's index and iterate each item.
        """

        response = self.getIndexes()

        for index in response["indexes"]["index"]:
            for index in index["artist"]:
                for item in self.walk_directory(index["id"]):
                    yield item

        for child in response["indexes"]["child"]:
            if child.get("isDir"):
                for child in self.walk_directory(child["id"]):
                    yield child
            else:
                yield child

    def walk_playlists(self):
        """
        Request SubSonic's playlists and iterate over each item.
        """

        response = self.getPlaylists()

        for child in response["playlists"]["playlist"]:
            yield child

    def walk_playlist(self, playlist_id):
        """
        Request SubSonic's playlist items and iterate over each item.
        """

        response = self.getPlaylist(playlist_id)

        for order, child in enumerate(response["playlist"]["entry"], start=1):
            child["order"] = order
            yield child

    def walk_directory(self, directory_id):
        """
        Request a SubSonic music directory and iterate over each item.
        """

        response = self.getMusicDirectory(directory_id)

        for child in response["directory"]["child"]:
            if child.get("isDir"):
                for child in self.walk_directory(child["id"]):
                    yield child
            else:
                yield child

    def walk_artist(self, artist_id):
        """
        Request a SubSonic artist and iterate over each album.
        """

        response = self.getArtist(artist_id)

        for child in response["artist"]["album"]:
            yield child
