# -*- coding:utf-8 -*-

import logging

from flask_restful import Api
from flask import Flask
from flask_json import FlaskJSON
from apps.feature_importance import Feature_importance


application = Flask(__name__)
FlaskJSON(application)

logger = logging.getLogger('feature_importance_log')
logger.setLevel(logging.INFO)

api = Api(application)
api.add_resource(Feature_importance, '/feature_importance', resource_class_kwargs={'logger': logger})