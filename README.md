# SubDaap
DAAP server/proxy for SubSonic: play your favorite tunes from SubSonic in iTunes!

The motivation for this application comes from the fact that SubSonic does not ship a DAAP server, and OS X clients for SubSonic lack features, in my opinion. And after all, iTunes is a pretty intuitive and stable player.

This project is an early and experimental version. I use it for my personal library of +/- 25.000 items, which works great and fast enough. It has not been tested on systems other than OS X.

## Features
* Compatible with SubSonic 5.2+ and iTunes 12+, including password protection and Bonjour.
* Artwork support.
* Playlist support.
* Browse the whole library in iTunes at once.
* Supports gapless playback.
* Smart file caching: supports in-file searching and concurrent access.
* Revision support: efficient library updates pushed to all connected clients.

## Requirements
* Python 2.7+ (not Python 3.x). PyPy 2.4 works and is a lot faster.
* SubSonic 5.2+

## Installation
This application was designed as a gateway between SubSonic and iTunes. Therefore, it's recommended to install this on the same system where you would access iTunes on. It can be installed on a central server, however.

* Clone this repository.
* Install dependencies via `pip install -r requirements.txt`.
* Copy `config.ini.default` to `config.ini` and edit as desired.
* `chmod 700 config.ini`, so others cannot view your credentials!

To start this service when your computer starts:
* On OS X
  * Copy `init/init.osx` to `~/Library/LaunchAgents/com.basilfx.subdaap.plist`. Do not symlink!
  * Edit the file accordingly. Make sure all paths are correct.
  * Run `launchctl load  ~/Library/LaunchAgents/com.basilfx.subdaap.plist`

## Run the application
To run the application, use the following command, or similar:

```
python SubDaap.py --config config.ini --data-dir path/to/datadir --pid-file /var/run/subdaap.pid
```

The data directory should exist. Optionally, add `-v` for verbose, or `-vv` for more verbose. All paths in the command line are relative to where you run it from.

Add `--daemon` to run the program in the background.

## Known issues
* Gevent may throw `Broken Pipe' exceptions. This is a known issue and can be ignored. See https://github.com/surfly/gevent/pull/377 for more information.

## License
See the `LICENSE` file (MIT license).