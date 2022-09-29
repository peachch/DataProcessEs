
from pipeline.core.engine import register
from pipeline.nodes_vendor.base_media import VideoMixin, AlbumMixin


@register
class Album(AlbumMixin):
    def __init__(self):
        self.vendor = "15"
        self.name = "album_tencent15"


@register
class Video(VideoMixin):
    def __init__(self):
        self.vendor = "15"
        self.name = "video_tencent15"
