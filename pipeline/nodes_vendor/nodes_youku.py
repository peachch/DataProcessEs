
from pipeline.core.engine import register
from pipeline.nodes_vendor.base_media import VideoMixin, AlbumMixin


@register
class Album(AlbumMixin):
    def __init__(self):
        self.vendor = "19"
        self.name = "album_youku"


@register
class Video(VideoMixin):
    def __init__(self):
        self.vendor = "19"
        self.name = "video_youku"
