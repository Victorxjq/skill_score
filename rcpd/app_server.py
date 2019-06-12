#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An example of a standalone application using the internal API of Gunicorn.
#
#   $ python standalone_app.py
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# import multiprocessing

import gunicorn.app.base
from apps.route_apps import application


def number_of_workers():
    # return (multiprocessing.cpu_count() * 2) + 1
    return 2

class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {key: value for key, value in self.options.items()
                  if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


if __name__ == '__main__':
    options = {
        'bind': '%s:%s' % ('0.0.0.0', '9001'),
        'workers': number_of_workers(),
    }
    StandaloneApplication(application, options).run()