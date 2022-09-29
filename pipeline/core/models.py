
from consecution import Pipeline
from retry import retry


class MediaProcessPipeline(Pipeline):

    @retry(3)
    def consume(self, media):
        """
        The pipeline will process media.
        """
        self.begin()
        self.top_node._process(media)
        self.end()


class Media(object):
    vendor = None
    name = None
    media_type = None

    def process_node(self):
        """
        流处理逻辑,返回媒资处理节点Nodes
        """
        raise NotImplementedError

    @staticmethod
    def upload_rules():
        """
        定义媒资各个字段合并、上传规则
        """
        raise NotImplementedError
