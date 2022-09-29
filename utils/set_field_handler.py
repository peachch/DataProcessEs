from tqdm import tqdm
import settings
from common.mongo_helper import MongoHelper


class FieldHacker(object):
    """利用标注数据补充或者删除媒资标签"""

    def __init__(self, field_name):
        self.mongo_conn = MongoHelper()
        self.tagging_coll = settings.REFER_TAGGING
        self.pub_coll = settings.PUB_ALBUM
        self.field_name = field_name

    def fix_item(self, media_id, new_value):
        """修正单个值的字段"""
        field_mapping = {"title": str,
                         "video_name": str,
                         "entitle": str,
                         "brief": str,
                         "publishdate": int,
                         "reference_time": int,
                         "publishyear": int,
                         "score": int,
                         "season": str}
        assert self.field_name in field_mapping
        new_value = field_mapping[self.field_name](new_value)

        matched_doc = self.mongo_conn.find_one(coll=self.tagging_coll, dict_filter={"id": media_id})
        if not matched_doc:
            matched_doc = self.generate_tagging_doc(media_id=media_id)

        self.mongo_conn.update(coll=self.tagging_coll, dict_filter={"id": media_id},
                               dict_update={"$set": {f"reference.{self.field_name}": new_value,
                                                     f"correct.{self.field_name}_fix": new_value}})  # 修正到指定值
        tqdm.write(f"mediaID: {media_id}, new value: {new_value}")

    def add_items(self, media_id, items_add: list):
        """添加数组字段元素"""
        assert self.field_name in ["fullpinyin", "simplepinyin", "aliases", "regions", "languages", "tags", "actors",
                                   "directors", "producers", "characters", "awards", "writer"]
        items_add = list(set(items_add))

        # 查找标注媒资是否已经存在
        matched_doc = self.mongo_conn.find_one(coll=self.tagging_coll, dict_filter={"id": media_id})
        if not matched_doc:
            matched_doc = self.generate_tagging_doc(media_id=media_id)

        items_remove = [t for t in set(matched_doc["correct"].get(f"{self.field_name}_blacklist", [])) if
                        t not in items_add]
        self.mongo_conn.update(coll=self.tagging_coll, dict_filter={"id": media_id},
                               dict_update={"$addToSet": {f"reference.{self.field_name}": {"$each": items_add}},
                                            "$set": {f"correct.{self.field_name}_blacklist": items_remove}})  # 添加标签
        tqdm.write(f"mediaID: {media_id}, added items: {items_add}")

    def remove_items(self, media_id, items_remove: list):
        """移除数组字段元素"""
        assert self.field_name in ["fullpinyin", "simplepinyin", "aliases", "regions", "languages", "tags", "actors",
                                   "directors", "producers", "characters", "awards", "writer"]
        items_remove = list(set(items_remove))
        matched_doc = self.mongo_conn.find_one(coll=self.tagging_coll, dict_filter={"id": media_id})
        if not matched_doc:
            matched_doc = self.generate_tagging_doc(media_id=media_id)

        all_items_remove = list(set(items_remove + matched_doc["correct"].get(f"{self.field_name}_blacklist", [])))
        self.mongo_conn.update(coll=self.tagging_coll, dict_filter={"id": media_id},
                               dict_update={"$set": {f"correct.{self.field_name}_blacklist": all_items_remove},
                                            "$pull": {f"reference.{self.field_name}": {"$in": items_remove}}})  # 移除标签
        tqdm.write(f"mediaID: {media_id}, removed items: {items_remove}")

    def generate_tagging_doc(self, media_id):
        """生成标注数据"""

        new_tagging_doc = dict(id=media_id, reference={}, correct={})
        # 尝试通过已发布的媒资构建一条标注数据

        existed_media = self.mongo_conn.find_one(coll=self.pub_coll, dict_filter={"id": media_id})
        if existed_media:
            new_tagging_doc.update(vendor=existed_media["vendor"], aid=existed_media["aid"])
            for field in ['title', 'video_name', 'entitle', 'brief', 'publishdate', 'reference_time', 'publishyear',
                          'score', 'season', 'fullpinyin', 'simplepinyin', 'aliases', 'regions', 'languages', 'tags',
                          'actors', 'directors', 'producers', 'characters', 'awards', 'writer']:
                if field in existed_media:
                    new_tagging_doc["reference"].update({field: existed_media[field]})

        # 生成一条新的标注数据
        self.mongo_conn.replace_one(coll=self.tagging_coll, dict_filter={"id": media_id},
                                    dict_replacement=new_tagging_doc)
        return new_tagging_doc


if __name__ == '__main__':
    h = FieldHacker(field_name="reference_time")
    h.fix_item(media_id="15sbif609lnu6zd94", new_value=2017)
