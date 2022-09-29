

MONGODB_HOST = "127.0.0.1"
MONGODB_PORT = 27017

# 媒资topic
ALBUM_TOPIC = "ffalcon_album_records_v2"
VIDEO_TOPIC = "ffalcon_video_records_v2"

# 从kafka中获取媒资时使用的group
ALBUM_GROUP_ID = "album_210719"
VIDEO_GROUP_ID = "video_210719"

PRODUCTION_SZ_ES = "http:///"
PRODUCTION_BJ_ES = "http:///"
ELK_ES = "http://:@127.0.0.1:000/"

# 上传用户分众的es
USER_GROUP_ES_SERVERS = [PRODUCTION_SZ_ES, PRODUCTION_BJ_ES]  # 需要同步到的服务器

# 上传媒资的es
MEDIA_ES_SERVERS = [PRODUCTION_BJ_ES, PRODUCTION_SZ_ES, ELK_ES]