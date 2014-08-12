# SubDaap
DAAP server/proxy for SubSonic: play your favorite tunes from SubSonic in
iTunes!

The motivation for this application comes from the fact that SubSonic does not
ship a DAAP server, and OS X clients for SubSonic lack features, in my opinion.
And after all, iTunes is a pretty intuitive and stable player.

This project is an early and experiomental version. It hasn't been tested on
systems other than OSX.

## Features
* Compatible with SubSonic 4.9+ and iTunes 11+, including password protection and Bonjour
* Artwork support
* Playlist support
* Browse the whole library in iTunes
* Supports gapless playback
* Smart file caching: supports in-file searching and concurrent access.
* Revision support: efficient library updates pushed to all connected clients

## Requirements
* Python 2.7+. PyPy 2.3 works and is a lot faster, but has some issues with Bonjour.
* Database provider. SQLite works, but has some issues. MySQL is fine

## Installation
This application was designed as a gateway between SubSonic and iTunes.
Therefore, it's recommended to install this on the same system where you would
access iTunes on. It can be installed on a central server, however.

* Clone this repository
* Install dependencies:
  * `pip install git+https://github.com/crustymonkey/py-sonic.git`
  * `pip install git+https://github.com/basilfx/flask-daapserver.git`
  * `pip install git+https://github.com/basilfx/pybonjour-python3`
  * `pip install gevent` (for Pypy: `pip install git+https://github.com/surfly/gevent`)
  * `pip install flask`
  * `pip install sqlalchemy`
  * `pip install configobj`
  * `pip install mysql-python` (for MySQL support)
* Copy `config.ini.default` to `config.ini`
  * Don't forget to change the database settings!
* `chmod 700 config.ini`, so others cannot view your credentials!

## Run the application
To run the application, use the following command, or similar:

```
python SubDaap.py -D --config config.ini --data-dir path/to/datadir --pid-file /var/run/subdaap.pid
```

The data directory should exist. Optionally, add `-v` for verbose, or `-vvv` for
more verbose.

## Known issues
* SQLite doesn't work when application runs as daemon.
* Titles, artists and/or albums may have HTML entities in them. This is due to a bug in libsonic.

## License
See the `LICENSE` file (MIT license).