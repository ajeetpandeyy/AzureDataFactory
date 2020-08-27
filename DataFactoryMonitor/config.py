import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):

    ES_EN_KEY = os.environ.get('ES_EN_KEY')
    ZH_EN_KEY = os.environ.get('ZH_EN_KEY')
    COG_SVCS_KEY = os.environ.get('COG_SVCS_KEY')
    SECRET_KEY = os.urandom(32)
