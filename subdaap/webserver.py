from subdaap import utils

from flask import render_template

import jinja2
import os

def extend_server_app(application, app):
    """
    Since the DAAP server is basically a normal HTTP server, extend the it with
    a webinterface for easy access and stats.
    """

    # Set the jinja2 loader
    template_path = os.path.join(os.path.dirname(__file__), "templates")
    app.jinja_loader = jinja2.ChoiceLoader([ app.jinja_loader, jinja2.FileSystemLoader(template_path) ])

    app.jinja_env.filters["human_bytes"] = utils.human_bytes

    @app.route("/")
    def index():
        return render_template("index.html", application=application)