import settings
from  pipeline.nodes_vendor import *
from pipeline.core.engine import worker


# 默认发布到生产和预生产
worker(vendor="13", media_type="album")