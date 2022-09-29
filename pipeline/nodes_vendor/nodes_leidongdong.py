
from copy import copy
from pipeline.core.engine import register
from pipeline.nodes_vendor.base_media import AlbumMixin
from pipeline.nodes_vendor.base_node_album import AlbumPreprocessNode, TitleCleanNode, BaseNode, \
    AlbumUpdateRelativeNode, MergeTaggingNode, LeiniaoBlackListNode


class TagsCleanNode(BaseNode):
    """雷咚咚数据标签字段清洗"""

    def process_media(self, item: dict):
        # 预处理做了字段名小写处理,雷咚咚使用指定的voicetags字段作为语音搜索标签
        item["tags"] = copy(item["ext"]["voicetags"])

        # VUI-8002 补充"初三"标签
        for tag in ["初一", "初二", "初三", "真人版", "剧场版"]:
            if tag in item["title"]:
                item["tags"].append(tag)
        item["tags"] = list(set(item["tags"]))

        return item


class SignRepetitiveNode(BaseNode):
    """相同媒资屏蔽需求"""

    def process_media(self, item: dict):
        # 雷咚咚少儿屏蔽需求，有点蠢
        item["repetitive_items_prior_eliminated"] = []
        return item


class ResetTypeNode(BaseNode):
    """雷咚咚所有专辑类型均为正片"""

    def process_media(self, item: dict):
        # 存在未定义type字段的数据, 雷咚咚专辑全部为正片
        item["type"] = 1
        return item


@register
class Album(AlbumMixin):
    def __init__(self):
        self.vendor = "310"
        self.name = "album_leidongdong"

    def process_node(self):
        """专辑处理节点"""
        preprocess_node = AlbumPreprocessNode(f'{self.name}_preprocess')  # 数据预处理
        title_cleaning_node = TitleCleanNode(f"{self.name}_clean_title")  # 标题清洗
        tags_cleaning_node = TagsCleanNode(f"{self.name}_clean_tags")  # 自定义清洗规则
        merge_tagging_node = MergeTaggingNode(f"{self.name}_merge_tagging")  # 标注部分season字段
        sign_repetitive_node = SignRepetitiveNode(f"{self.name}_sign_rep")  # 不同牌照之间去重
        reset_type_node = ResetTypeNode(f"{self.name}_reset_type")  # 雷咚咚所有专辑均为正片
        clear_blacklist_media = LeiniaoBlackListNode(f"{self.name}_blacklist")  # 下架雷咚咚黑名单
        update_relative_node = AlbumUpdateRelativeNode(f"{self.name}_update_relative")  # 更新关联

        node_graph = preprocess_node | title_cleaning_node | tags_cleaning_node | merge_tagging_node | sign_repetitive_node | reset_type_node | clear_blacklist_media | update_relative_node
        return node_graph
