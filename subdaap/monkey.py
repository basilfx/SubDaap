from gevent.monkey import patch_all


def patch_pypy():
    """
    Monkey patch PyPy so SubDaap works.
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

    # Patch for Sqlite3 threading issue. Since SubDaap uses greenlets
    # (microthreads) and no actual threads, disable the warning. This only
    # happens with PyPy.
    import sqlite3

    old_connect = sqlite3.connect
    sqlite3.connect = lambda x: old_connect(x, check_same_thread=False)


def patch_ssl():
    """
    Monkey patch SSL to fix the SSL warp socket method, that fails since Python
    2.7.9. See https://github.com/gevent/gevent/issues/477 for more info.
    """

    import inspect
    __ssl__ = __import__("ssl")

    try:
        _ssl = __ssl__._ssl
    except AttributeError:
        _ssl = __ssl__._ssl2

    def new_sslwrap(sock, server_side=False, keyfile=None, certfile=None,
                    cert_reqs=__ssl__.CERT_NONE,
                    ssl_version=__ssl__.PROTOCOL_SSLv23, ca_certs=None,
                    ciphers=None):

        context = __ssl__.SSLContext(ssl_version)
        context.verify_mode = cert_reqs or __ssl__.CERT_NONE

        if ca_certs:
            context.load_verify_locations(ca_certs)
        if certfile:
            context.load_cert_chain(certfile, keyfile)
        if ciphers:
            context.set_ciphers(ciphers)

        caller_self = inspect.currentframe().f_back.f_locals["self"]

        return context._wrap_socket(
            sock, server_side=server_side, ssl_sock=caller_self)

    if not hasattr(_ssl, "sslwrap"):
        _ssl.sslwrap = new_sslwrap

# Apply all patches
patch_all()
patch_pypy()
patch_ssl()
