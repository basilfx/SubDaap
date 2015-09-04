from subdaap.utils import human_bytes, exhaust
from subdaap import stream

from collections import OrderedDict

import logging
import gevent
import time
import mmap
import os

# Logger instance
logger = logging.getLogger(__name__)

# Time in seconds to wait for another item to finish, before failing.
TIMEOUT_WAIT_FOR_READY = 60


class FileCacheItem(object):
    __slots__ = (
        "lock", "ready", "uses", "size", "type", "iterator", "data",
        "permanent"
    )

    def __init__(self):
        self.lock = None
        self.ready = None
        self.uses = 0

        self.size = 0
        self.iterator = None
        self.data = None
        self.permanent = False


class FileCache(object):
    def __init__(self, path, max_size, prune_threshold):
        """
        Construct a new file cache.

        :param str path: Path to the cache folder.
        :param int max_size: Maximum cache size (in MB), or 0 to disable.
        :param float prune_threshold: Percentage of size to prune when cache
                                      size exceeds maximum size.
        """

        # This attribute is used so often, it makes the code less long.
        self.name = self.__class__.__name__

        self.path = path
        self.max_size = max_size * 1024 * 1024
        self.prune_threshold = prune_threshold
        self.current_size = 0

        self.items = OrderedDict()
        self.items_lock = gevent.lock.Semaphore()
        self.prune_lock = gevent.lock.Semaphore()

        self.permanent_cache_keys = None

    def index(self, permanent_cache_keys):
        """
        Read the cache folder and determine its size by summing the file
        size of its contents.
        """

        self.permanent_cache_keys = permanent_cache_keys

        # Walk all files and sum their size
        for root, directories, files in os.walk(self.path):
            if directories:
                logger.warning(
                    "%s: Found unexpected directories in cache path: %s",
                    self.name, root)

            for cache_file in files:
                try:
                    cache_file = os.path.join(self.path, cache_file)
                    cache_key = self.cache_file_to_cache_key(cache_file)
                except ValueError:
                    logger.warning(
                        "%s: Found unexpected file in cache path: %s",
                        self.name, cache_file)
                    continue

                permanent = cache_key in permanent_cache_keys

                if cache_key not in self.items:
                    self.items[cache_key] = FileCacheItem()
                    self.items[cache_key].size = os.stat(cache_file).st_size
                    self.items[cache_key].permanent = permanent

        # Sum sizes of all non-permanent files
        size = 0
        count = 0

        for item in self.items.itervalues():
            if not item.permanent:
                size += item.size
                count += 1

        self.current_size = size

        logger.debug(
            "%s: %d files in cache (%d permanent), size is %s/%s",
            self.name, len(self.items), len(self.items) - count,
            human_bytes(self.current_size), human_bytes(self.max_size))

    def cache_key_to_cache_file(self, cache_key):
        """
        Get complete path to cache file, given a cache key.

        :param str cache_key:
        """
        return os.path.join(self.path, str(cache_key))

    def cache_file_to_cache_key(self, cache_file):
        """
        Get cache key, given a cache file.

        :param str cache_file:
        """
        return int(os.path.basename(cache_file))

    def get(self, cache_key):
        """
        Get item from the cache.

        :param str cache_key:
        """

        # Load item from cache. If it is found in cache, move it on top of the
        # OrderedDict, so it is marked as most-recently accessed and therefore
        # least likely to get pruned.
        new_item = False
        wait_for_ready = True

        # The lock is required to make sure that two concurrent I/O bounded
        # greenlets do not add or remove items at the same time (for instance
        # `self.prune`).
        with self.items_lock:
            try:
                cache_item = self.items[cache_key]
                del self.items[cache_key]
                self.items[cache_key] = cache_item
            except KeyError:
                self.items[cache_key] = cache_item = FileCacheItem()
                cache_item.permanent = cache_key in self.permanent_cache_keys
                new_item = True

            # The item can be either new, or it could be unloaded in the past.
            if cache_item.ready is None or cache_item.lock is None:
                cache_item.ready = gevent.event.Event()
                cache_item.lock = gevent.lock.RLock()
                wait_for_ready = False

            # The file is not in cache, but we allocated an instance so the
            # caller can load it. This is actually needed to prevent a second
            # request from also loading it, hence cache_item not ready.
            if new_item:
                return cache_item

        # Wait until the cache_item is ready for use, e.g. another request is
        # downloading the file.
        if wait_for_ready:
            logger.debug(
                "%s: waiting for item '%s' to be ready.", self.name, cache_key)

            if not cache_item.ready.wait(timeout=TIMEOUT_WAIT_FOR_READY):
                raise Exception("Waiting for cache item timed out.")

            # This may happen when some greenlet is waiting for the item to
            # become ready after its flag was cleared in the expire method. It
            # is probably possible to recover by re-iterating this method, but
            # first want to make sure if this situation is likely to happen.
            if cache_item.ready is None:
                raise Exception("Item unloaded while waiting.")

        # Load the item from disk if it is not loaded. The lock is needed to
        # prevent two concurrent requests from both loading a cache item.
        if cache_item.iterator is None:
            cache_item.ready.clear()
            self.load(cache_key, cache_item)

        return cache_item

    def contains(self, cache_key):
        """
        Check if a certain cache key is in the cache.
        """

        with self.items_lock:
            return cache_key in self.items

    def clean(self, force=False):
        """
        Prune items from the cache, if `self.current_size' exceeds
        `self.max_size`. Only items that have been expired will be pruned,
        unless it is marked as permanent.

        :param bool force: If true, clean all items except permanent ones.
        """

        candidates = []

        with self.items_lock:
            for cache_key, cache_item in self.items.iteritems():
                if cache_item.uses == 0:
                    # Check if it is getting ready, e.g. one greenlet is
                    # downloading the file.
                    if cache_item.ready and not cache_item.ready.is_set():
                        continue

                    # Item was ready and not in use, therefore clear the ready
                    # flag so no one will use it.
                    if cache_item.ready:
                        cache_item.ready.clear()

                    candidates.append((cache_key, cache_item))

        for cache_key, cache_item in candidates:
            self.unload(cache_key, cache_item)

            cache_item.iterator = None
            cache_item.lock = None
            cache_item.ready = None

        if candidates:
            logger.debug("%s: expired %d files", self.name, len(candidates))

        candidates = []

        with self.prune_lock, self.items_lock:
            # Check if cleanup is required.
            if force:
                logger.info(
                    "%s: force cleaning all items from cache, except items "
                    "that are in use or marked as permanent.", self.name)
            else:
                if not self.max_size or self.current_size < self.max_size:
                    return

            # Determine candidates to remove.
            for cache_key, cache_item in self.items.iteritems():
                if not force:
                    if self.current_size < \
                            (self.max_size * (1.0 - self.prune_threshold)):
                        break

                # keep permanent items
                if cache_item.permanent:
                    continue

                # If `cache_item.ready` is not set, it is not loaded into
                # memory.
                if cache_item.ready is None:
                    candidates.append((cache_key, cache_item))
                    self.current_size -= cache_item.size

                    del self.items[cache_key]

        # Actual removal of the files. At this point, the cache_item is not in
        # `self.items` anymore. No other greenlet can retrieve it anymore.
        for cache_key, cache_item in candidates:
            cache_file = self.cache_key_to_cache_file(cache_key)

            try:
                os.remove(cache_file)
            except OSError as e:
                logger.warning(
                    "%s: unable to remove file '%s' from cache: %s",
                    self.name, os.path.basename(cache_file), e)

        if candidates:
            logger.debug(
                "%s: pruned %d files, current size %s/%s (%d files).",
                self.name, len(candidates), human_bytes(self.current_size),
                human_bytes(self.max_size), len(self.items))

    def update(self, cache_key, cache_item, cache_file, file_size):
        if cache_item.size != file_size:
            if cache_item.size:
                logger.warning(
                    "%s: file size of item '%s' changed from %d bytes to %d "
                    "bytes while it was in cache.", self.name, cache_key,
                    cache_item.size, file_size)

            if not cache_item.permanent:
                self.current_size -= cache_item.size
                self.current_size += file_size

            cache_item.size = file_size

    def download(self, cache_key, cache_item, remote_fd):
        start = time.time()

        def on_cache(file_size):
            """
            Executed when download finished. This method is executed with the
            `cache_item` locked.
            """

            logger.debug(
                "%s: downloading '%s' took %.2f seconds.", self.name,
                cache_key, time.time() - start)

            remote_fd.close()
            self.load(cache_key, cache_item)

        cache_file = self.cache_key_to_cache_file(cache_key)
        cache_item.iterator = stream.stream_from_remote(
            cache_item.lock, remote_fd, cache_file, on_cache=on_cache)


class ArtworkCache(FileCache):
    def load(self, cache_key, cache_item):
        cache_file = self.cache_key_to_cache_file(cache_key)

        def on_start():
            cache_item.uses += 1
            logger.debug(
                "%s: incremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        def on_finish():
            cache_item.uses -= 1
            logger.debug(
                "%s: decremented '%s' use to %d", self.name, cache_key,
                cache_item.uses)

        file_size = os.stat(cache_file).st_size
        cache_item.data = local_fd = open(cache_file, "rb")

        # Update cache item
        self.update(cache_key, cache_item, cache_file, file_size)

        cache_item.iterator = stream.stream_from_file(
            cache_item.lock, local_fd, file_size,
            on_start=on_start, on_finish=on_finish)
        cache_item.ready.set()

    def unload(self, cache_key, cache_item):
        if cache_item.data:
            cache_item.data.close()
            cache_item.data = None


class ItemCache(FileCache):
    def load(self, cache_key, cache_item):
        cache_file = self.cache_key_to_cache_file(cache_key)

        def on_start():
            cache_item.uses += 1
            logger.debug(
                "%s: incremented '%s' use to %d.", self.name, cache_key,
                cache_item.uses)

        def on_finish():
            cache_item.uses -= 1
            logger.debug(
                "%s: decremented '%s' use to %d.", self.name, cache_key,
                cache_item.uses)

        file_size = os.stat(cache_file).st_size

        local_fd = open(cache_file, "r+b")
        mmap_fd = mmap.mmap(local_fd.fileno(), 0, prot=mmap.PROT_READ)
        cache_item.data = local_fd, mmap_fd

        # Update cache item
        self.update(cache_key, cache_item, cache_file, file_size)

        cache_item.iterator = stream.stream_from_buffer(
            cache_item.lock, mmap_fd, file_size,
            on_start=on_start, on_finish=on_finish)
        cache_item.ready.set()

    def unload(self, cache_key, cache_item):
        if cache_item.data:
            local_fd, mmap_fd = cache_item.data

            mmap_fd.close()
            local_fd.close()

            cache_item.data = None


class CacheManager(object):
    """
    """

    def __init__(self, db, item_cache, artwork_cache):
        self.db = db
        self.item_cache = item_cache
        self.artwork_cache = artwork_cache

    def get_cached_items(self):
        """
        Get all items that should be permanently cached, independent of which
        database.
        """

        with self.db.get_cursor() as cursor:
            return cursor.query_dict(
                """
                SELECT
                    `items`.`id`,
                    `items`.`database_id`,
                    `items`.`remote_id`,
                    `items`.`file_suffix`
                FROM
                    `items`
                WHERE
                    `items`.`cache` = 1 AND
                    `items`.`exclude` = 0
                """)

    def cache(self):
        """
        Update the caches with all items that should be permanently cached.
        """

        cached_items = self.get_cached_items()

        self.artwork_cache.index(cached_items)
        self.item_cache.index(cached_items)

        logger.info("Caching %d permanent items.", len(cached_items))

        for local_id in cached_items:
            logger.debug("Caching item '%d'.", local_id)
            database_id, remote_id, file_suffix = cached_items[local_id]

            # Artwork
            if not self.artwork_cache.contains(local_id):
                cache_item = self.artwork_cache.get(local_id)

                if cache_item.ready is None:
                    remote_fd = self.connections[database_id].getCoverArt(
                        remote_id)
                    self.artwork_cache.download(
                        local_id, cache_item, remote_fd)

                    # Exhaust iterator so it downloads the artwork.
                    exhaust(cache_item.iterator())
                self.artwork_cache.unload(local_id)

            # Items
            if not self.item_cache.contains(local_id):
                cache_item = self.item_cache.get(local_id)

                if cache_item.ready is None:
                    remote_fd = self.get_item_fd(
                        database_id, remote_id, file_suffix)
                    self.item_cache.download(
                        local_id, cache_item, remote_fd)

                    # Exhaust iterator so it downloads the item.
                    exhaust(cache_item.iterator())
                self.item_cache.unload(local_id)

        logger.info("Caching permanent items finished.")

    def clean(self):
        """
        """

        self.item_cache.clean()
        self.artwork_cache.clean()
