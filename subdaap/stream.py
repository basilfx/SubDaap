from daapserver.utils import parse_byte_range

import os
import shutil
import gevent

def stream_from_remote(lock, remote_fd, target_file, chunk_size=8192,
    on_cache=None):
    """
    Spawn a greenlet to download and cache a file, while simultaniously stream
    data to the receiver. An additional greenlet is spawned to handle the file
    download and caching. Every time another block (of interest, depending on
    start and stop) is available, it will be written to the queue. The streamer
    blocks until a block of interest is available.
    """

    temp_file = "%s.temp" % target_file
    queue = gevent.queue.Queue()

    def _downloader():
        exhausted = False
        bytes_read = 0

        with open(temp_file, "wb") as local_fd:
            try:
                while True:
                    chunk = remote_fd.read(chunk_size)

                    if not chunk:
                        exhausted = True
                        break

                    local_fd.write(chunk)
                    bytes_read += len(chunk)

                    # Yield in the form of (chunk_begin, chunk_end, chunk)
                    yield bytes_read - len(chunk), bytes_read, chunk
            finally:
                # Make sure the remaining bytes are read from remote and
                # written to disk.
                if not exhausted:
                    while True:
                        chunk = remote_fd.read(chunk_size)

                        if not chunk:
                            break

                        local_fd.write(chunk)

                # Move the temp file to the target file. On the same disk, this
                # should be an atomic operation.
                shutil.move(temp_file, target_file)

                # Mark done, for the on_cache
                exhausted = True

        # Invoke callback, if fully exhausted
        if exhausted and on_cache:
            on_cache()

    def _cacher(begin, end):
        put = False

        # Hack (1)
        old_owner, lock._owner = lock._owner, gevent.getcurrent()

        with lock:
            try:
                for chunk_begin, chunk_end, chunk in _downloader():
                    if (chunk_begin <= begin < chunk_end) or (chunk_begin <= end < chunk_end):
                        put = not put

                    if put:
                        queue.put((chunk_begin, chunk_end, chunk))
            finally:
                # Make sure the streamer stops
                queue.put(StopIteration)

        # Hack (2)
        lock._owner = old_owner

    def _streamer(byte_range=None):
        begin, end = parse_byte_range(byte_range)

        # Spawn the download greenlet
        greenlet = gevent.spawn(_cacher, begin, end)

        try:
            put = False

            for chunk_begin, chunk_end, chunk in queue:
                if (chunk_begin <= begin < chunk_end) or (chunk_begin <= end < chunk_end):
                    put = not put

                if put:
                    i = max(0, begin - chunk_begin)
                    j = min(len(chunk), end - chunk_begin)

                    yield chunk[i:j]
        finally:
            # Make sure the greenlet gets killed when this iterator is closed
            greenlet.kill()

    return _streamer

def stream_from_file(lock, fd, file_size, on_start=None, on_finish=None):
    """
    Create an iterator that streams a file partially or all at once.
    """

    def _streamer(byte_range=None):
        begin, end = parse_byte_range(byte_range, max_byte=file_size)

        try:
            if on_start:
                on_start()

            with lock:
                fd.seek(begin)
                chunk = fd.read(end - begin)

            yield chunk
        finally:
            if on_finish:
                on_finish()

    return _streamer

def stream_from_buffer(lock, data, file_size, chunk_size=8192, on_start=None,
    on_finish=None):
    """
    """

    def _streamer(byte_range=None):
        begin, end = parse_byte_range(byte_range, max_byte=file_size)

        # Yield data in chunks
        try:
            if on_start:
                on_start()

            while True:
                with lock:
                    chunk = data[begin:min(end, begin + chunk_size)]

                # Send the data
                yield chunk

                # Increment offset
                begin += len(chunk)

                # Stop when the end has been reached
                if begin >= end:
                    break
        finally:
            if on_finish:
                on_finish()

    return _streamer