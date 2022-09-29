

from pipeline.core.engine import register
from pipeline.nodes_vendor.base_media import VideoMixin, AlbumMixin


# todo 小米aid和其它牌照方重复且没有任何相关性332508

@register
class MiAlbum(AlbumMixin):
    def __init__(self):
        self.vendor = "17"
        self.name = "album_mi"


@register
class MiVideo(VideoMixin):
    def __init__(self):
        self.vendor = "17"
        self.name = "video_mi"
