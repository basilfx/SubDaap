from subdaap import utils

from flask import render_template, redirect, url_for

import logging
import jinja2
import os

# Logger instance
logger = logging.getLogger(__name__)


def extend_server_app(application, app):
    """
    Since the DAAP server is basically a normal HTTP server, extend the it with
    a webinterface for easy access and stats.
    """

    # Set the jinja2 loader
    template_path = os.path.join(os.path.dirname(__file__), "templates")
    app.jinja_loader = jinja2.ChoiceLoader([
        app.jinja_loader, jinja2.FileSystemLoader(template_path)])

    app.jinja_env.filters["human_bytes"] = utils.human_bytes

    @app.route("/")
    @app.authenticate
    def index():
        """
        Default index.
        """

        return render_template("index.html", application=application)

    @app.route("/actions/<action>")
    @app.authenticate
    def actions(action):
        """
        Handle actions and return to index page.
        """

        action = action.lower()
        logger.info("Webserver action: %s", action)

        if action == "shutdown":
            application.stop()
        elif action == "synchronize":
            if not application.provider.lock.locked():
                application.provider.synchronize()
            else:
                logger.warn("Provider is still locked. Already synchronizing?")

        # Return back to index
        return redirect(url_for("index"))
