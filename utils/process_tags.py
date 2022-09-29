from tqdm import tqdm
import settings
from common.mongo_helper import MongoHelper


class TagsHacker(object):
    """利用标注数据补充或者删除媒资标签"""

    def __init__(self):
        self.mongo_conn = MongoHelper()
        self.tagging_coll = settings.REFER_TAGGING
        self.pub_coll = settings.PUB_ALBUM

    def add_tags(self, media_id, tags_add: list):
        """添加标签"""
        tags_add = list(set(tags_add))

        # 查找标注媒资是否已经存在
        matched_doc = self.mongo_conn.find_one(coll=self.tagging_coll, dict_filter={"id": media_id})
        if not matched_doc:
            matched_doc = self.generate_tagging_doc(media_id=media_id)

        tags_remove = [t for t in set(matched_doc["correct"].get("tags_blacklist", [])) if t not in tags_add]
        self.mongo_conn.update(coll=self.tagging_coll, dict_filter={"id": media_id},
                               dict_update={"$addToSet": {"reference.tags": {"$each": tags_add}},
                                            "$set": {"correct.tags_blacklist": tags_remove}})  # 添加标签
        tqdm.write(f"mediaID: {media_id}, added tags: {tags_add}")

    def remove_tags(self, media_id, tags_remove: list):
        """移除标签"""
        tags_remove = list(set(tags_remove))
        matched_doc = self.mongo_conn.find_one(coll=self.tagging_coll, dict_filter={"id": media_id})
        if not matched_doc:
            matched_doc = self.generate_tagging_doc(media_id=media_id)

        all_tags_remove = list(set(tags_remove + matched_doc["correct"].get("tags_blacklist", [])))
        self.mongo_conn.update(coll=self.tagging_coll, dict_filter={"id": media_id},
                               dict_update={"$set": {"correct.tags_blacklist": all_tags_remove},
                                            "$pull": {"reference.tags": {"$in": tags_remove}}})  # 移除标签
        tqdm.write(f"mediaID: {media_id}, removed tags: {tags_remove}")

    def generate_tagging_doc(self, media_id):
        """生成标注数据"""

        new_tagging_doc = dict(id=media_id, reference={}, correct={})
        # 尝试通过已发布的媒资构建一条标注数据

        existed_media = self.mongo_conn.find_one(coll=self.pub_coll, dict_filter={"id": media_id})
        if existed_media:
            new_tagging_doc.update(vendor=existed_media["vendor"], aid=existed_media["aid"])
            for field in ['title', 'video_name', 'entitle', 'brief', 'publishdate', 'reference_time', 'publishyear', 'score', 'season', 'fullpinyin', 'simplepinyin', 'aliases', 'regions', 'languages', 'tags', 'actors', 'directors', 'producers', 'characters', 'awards', 'writer']:
                if field in existed_media:
                    new_tagging_doc["reference"].update({field: existed_media[field]})

        # 生成一条新的标注数据
        self.mongo_conn.replace_one(coll=self.tagging_coll, dict_filter={"id": media_id},
                                    dict_replacement=new_tagging_doc)
        return new_tagging_doc


if __name__ == '__main__':
    hacker = TagsHacker()
    hacker.add_tags(media_id="15sbif609lnu6zd94", tags_add=["丧尸"])
    hacker.remove_tags(media_id="15sbif609lnu6zd94", tags_remove=["僵尸"])
