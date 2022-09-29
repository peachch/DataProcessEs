
import re

from pipeline.core.engine import register
from pipeline.nodes_vendor import VideoPreprocessNode, VideoExtractSeasonNode, VideoExplodeNode
from pipeline.nodes_vendor.base_media import VideoMixin, AlbumMixin
from pipeline.nodes_vendor.base_node_video import BaseNode
from utils.data_utils import chinese2num


@register
class Album(AlbumMixin):
    def __init__(self):
        self.vendor = "22"
        self.name = "album_bili"


@register
class Video(VideoMixin):  # todo 大量分集没有标题，所有index都为0
    def __init__(self):
        self.vendor = "22"
        self.name = "video_bili"

    def process_node(self):
        """分集处理节点"""
        extract_index_node = ExtractIndexNode(f"{self.name}_extract_index")
        preprocess_node = VideoPreprocessNode(f"{self.name}_preprocess")
        extract_season_node = VideoExtractSeasonNode(f"{self.name}_field_season")  # 从标题提取season, 更新专辑数据
        explode_node = VideoExplodeNode(f"{self.name}_explode")
        node_graph = extract_index_node | preprocess_node | extract_season_node | explode_node
        return node_graph


class ExtractIndexNode(BaseNode):
    """从标题抽取index"""

    def process_media(self, item: dict):
        if item["index"]:
            return item

        index = 0
        isdigitMatched = re.search(r"^(?P<index>[一二三四五六七八九十0-9]+)$", item["title"])
        matchedIndex = re.search(r"第(?P<index>[一二三四五六七八九十0-9]+)[集篇]", item["title"])

        if isdigitMatched:
            index = int(chinese2num(isdigitMatched.group("index")))
        elif matchedIndex:
            index = int(chinese2num(matchedIndex.group("index")))
        item["index"] = index
        return item
