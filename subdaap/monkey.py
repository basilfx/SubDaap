from gevent.monkey import patch_all


def patch_pypy():
    """
    Monkey patch PyPy so it SubDaap runs better.
    """

    # Check if running under PyPY
    try:
        import __pypy__ # noqa
    except ImportError:
        return

    # Patch for missing py3k acquire.
    # See https://github.com/gevent/gevent/issues/248 for more information.
    from gevent.lock import Semaphore

    if not hasattr(Semaphore, "_py3k_acquire"):
        Semaphore._py3k_acquire = Semaphore.acquire

    # Patch for Sqlite3 threading issue. Since SubDaap uses microthreads and
    # no actual threads, disable the warning. This only happens with PyPy.
    import sqlite3

    old_connect = sqlite3.connect
    sqlite3.connect = lambda x: old_connect(x, check_same_thread=False)

# Apply all patches
patch_all()
patch_pypy()
