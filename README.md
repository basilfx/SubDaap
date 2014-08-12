# SubDaap
DAAP server/proxy for SubSonic: play your favorite tunes from SubSonic in
iTunes!

This project is an early alpha and hasn't been tested systems other than OSX.

## Featuers
* Compatible with SubSonic 4.9+ and iTunes 11+, including password protection and Bonjour
* Artwork support
* Playlist support
* Browse the whole library in iTunes
* Smart file caching: supports in-file searching and concurrent access.
* Revision support: efficient library updates pushed to all connected clients

## Requirements
* Python 2.7+. PyPy 2.3 works and is a lot faster, but has some issues with Bonjour
* Database provider. SQLite works, but has some issues. MySQL is fine

## Installation
* Clone this repository
* Install dependencies:
  * `pip install git+https://github.com/crustymonkey/py-sonic.git`
  * `pip install git+https://github.com/basilfx/flask-daapserver.git`
  * `pip install git+https://github.com/depl0y/pybonjour-python3`
  * `pip install gevent`
  * `pip install sqlalchemy`
  * `pip install flask`
* Copy `config.ini.default` to `config.ini`
  * Don't forget to change the database settings!

## Run the application
To run the application, use the following command, or similar:

```
python SubDaap.py -D --config config.ini --data-dir path/to/datadir --pid-file /var/run/subdaap.pid
```

The data directory should exist. Optionally, add `-v` for verbose, or `-vvv` for
more verbose.

## Known issues
* SQLite doesn't work when application runs as daemon.

## License
See the `LICENSE` file (MIT license).