from subdaap import utils

from flask import Response
from flask import render_template, redirect, url_for, send_from_directory

import logging
import jinja2
import os

# Logger instance
logger = logging.getLogger(__name__)


def extend_server_app(application, app):
    """
    Since the DAAP server is basically a normal HTTP server, extend it with a
    web interface for easy access and statistics.

    If the DAAPServer was configured with a password, the default username is
    empty and the password is equal to the configured password.

    :param Application application: SubDaap application for information.
    :param Flask app: Flask/DAAPServer to extend.
    """

    # Set the jinja2 loader
    template_path = os.path.join(os.path.dirname(__file__), "templates")
    static_path = os.path.join(os.path.dirname(__file__), "static")

    app.jinja_loader = jinja2.ChoiceLoader([
        app.jinja_loader, jinja2.FileSystemLoader(template_path)])

    app.jinja_env.filters["human_bytes"] = utils.human_bytes

    @app.route("/")
    @app.authenticate
    def index():
        """
        Default index.
        """

        return render_template(
            "index.html", application=application,
            provider=application.provider)

    @app.route("/static/<path:path>")
    @app.authenticate
    def static(path):
        """
        Handle static files from the `static_path` folder.
        """

        return send_from_directory(static_path, path)

    @app.route("/actions/<action>")
    @app.authenticate
    def actions(action):
        """
        Handle actions and return to index page.
        """

        action = action.lower()
        logger.info("Webserver action: %s", action)

        # Shutdown action
        if action == "shutdown":
            application.stop()

        # Prune action
        elif action == "prune":
            if not application.provider.item_cache.prune_lock.locked():
                application.provider.item_cache.expire()
                application.provider.item_cache.prune(cleanup=True)
            else:
                logger.warn("Item cache is locked. Already pruning?")

            if not application.provider.artwork_cache.prune_lock.locked():
                application.provider.artwork_cache.expire()
                application.provider.artwork_cache.prune(cleanup=True)
            else:
                logger.warn("Artwork cache is locked. Already pruning?")

        # Synchronize action
        elif action == "synchronize":
            if not application.provider.lock.locked():
                application.provider.synchronize()
                application.provider.cache()
            else:
                logger.warn("Provider is locked. Already synchronizing?")

        # Return back to index
        return redirect(url_for("index"))

    @app.route("/raw/tree")
    @app.authenticate
    def raw_tree():
        """
        Print a raw tree of the current server storage.
        """

        generator = (x + "\n" for x in application.provider.server.to_tree())

        return Response(generator, mimetype="text/plain")
