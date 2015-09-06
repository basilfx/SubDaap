from gevent.monkey import patch_all


def patch_pypy():
    """
    Monkey patch PyPy so SubDaap works.
    """

    # Check if running under PyPY
    try:
        import __pypy__  # noqa
    except ImportError:
        return

    # Patch for missing py3k acquire.
    # See https://github.com/gevent/gevent/issues/248 for more information.
    from gevent.lock import Semaphore

    if not hasattr(Semaphore, "_py3k_acquire"):
        Semaphore._py3k_acquire = Semaphore.acquire

    # Patch for Sqlite3 threading issue. Since SubDaap uses greenlets
    # (microthreads) and no actual threads, disable the warning. This only
    # happens with PyPy.
    import sqlite3

    old_connect = sqlite3.connect
    sqlite3.connect = lambda x: old_connect(x, check_same_thread=False)


def patch_zeroconf():
    """
    Monkey patch Zeroconf so the select timeout can be disabled when running
    with gevent. Saves some wakeups.
    """

    import zeroconf

    def new_init(self, *args, **kwargs):
        old_init(self, *args, **kwargs)
        self.timeout = None

    old_init = zeroconf.Engine.__init__
    zeroconf.Engine.__init__ = new_init


# Apply all patches
patch_all()
patch_pypy()
patch_zeroconf()
