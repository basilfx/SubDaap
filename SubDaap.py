from gevent import monkey; monkey.patch_all()

from subdaap.utils import VerboseAction, PathAction, NewPathAction
from subdaap.application import Application

import argparse
import gevent
import logging
import atexit
import sys
import os

# Logger instance
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Parse commandline arguments.
    """

    parser = argparse.ArgumentParser()

    # Add options
    parser.add_argument("-D", "--daemon", action="store_true",
        help="run as daemon")
    parser.add_argument("-v", "--verbose", nargs="?", action=VerboseAction,
        default=0, help="toggle verbose mode (-vv, -vvv for more)")
    parser.add_argument("-c", "--config-file", action=PathAction,
        default="config.ini", help="config file")
    parser.add_argument("-d", "--data-dir", action=PathAction,
        default=os.getcwd(), help="data directory")
    parser.add_argument("-p", "--pid-file", action=NewPathAction,
        help="pid file")
    parser.add_argument("-l", "--log-file", action=NewPathAction,
        help="log file")

    # Parse command line
    return parser.parse_args(), parser

def setup_logging(verbose, log_file):
    """
    """

    # Setup logging
    logging.basicConfig(level=logging.DEBUG if verbose > 0 else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Verbose level is %d", verbose)

def daemonize(pid_file=None):
    """
    Daemonize the current process. Returns the PID of the continuing child
    process. As an extra option, the PID of the child process can be written to
    a specified pid file.
    """

    # Dependency check to make sure the imports are OK. Saves you from a lot of
    # debugging trouble.
    assert atexit.register and os.fork and sys.stdout

    # First fork
    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("Unable to fork: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Decouple from parent
    os.setsid()
    os.umask(0)

    # Second fork
    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("Unable to fork: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Redirect file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    stdin = file("/dev/null", "r")
    stdout = file("/dev/null", "a+")
    stderr = file("/dev/null", "a+", 0)

    os.dup2(stdin.fileno(), sys.stdin.fileno())
    os.dup2(stderr.fileno(), sys.stdout.fileno())
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    # Write PID file
    if pid_file:
        atexit.register(os.remove, pid_file)

        with open(pid_file, "w+") as fp:
            fp.write("%d" % os.getpid())

    # Return the PID
    return os.getpid()

def main():
    """
    Main entry point. Parses arguments, daemonizes and creates the application.
    """

    # Parse arguments and configure application instance.
    arguments, parser = parse_arguments()

    if arguments.daemon:
        daemonize(arguments.pid_file)

    setup_logging(arguments.verbose, arguments.log_file)

    # Change to data directory
    os.chdir(arguments.data_dir)

    # Create application instance and run it.
    try:
        application = Application(
            config_file=arguments.config_file, data_dir=arguments.data_dir,
            verbose=arguments.verbose)
    except Exception as e:
        logger.error("One or more components failed to initialize: %s", e)
        raise
        return 1

    try:
        application.start()
    except KeyboardInterrupt:
        application.stop()

    # Done
    return 0

# E.g. `python SubDaap.py --daemonize --config-file=config.ini"
if __name__ == "__main__":
    sys.exit(main())