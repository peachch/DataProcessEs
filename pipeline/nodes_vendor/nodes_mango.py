
from pipeline.core.engine import register
from pipeline.nodes_vendor.base_media import VideoMixin, AlbumMixin


@register
class MangoAlbum(AlbumMixin):
    def __init__(self):
        self.vendor = "12"
        self.name = "album_mango"


@register
class MangoVideo(VideoMixin):
    def __init__(self):
        self.vendor = "12"
        self.name = "video_mango"
