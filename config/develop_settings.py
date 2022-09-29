
DEVELOP_ES = "http:///"

# 上传用户分众的es
USER_GROUP_ES_SERVERS = [DEVELOP_ES, ]  # 需要同步到的服务器

# 上传媒资的es
MEDIA_ES_SERVERS = [DEVELOP_ES, ]

MONGODB_HOST = ""
MONGODB_PORT =

# 媒资topic
ALBUM_TOPIC = "ffalcon_album_records_test"
VIDEO_TOPIC = "ffalcon_video_records_test"

# 从kafka中获取媒资时使用的group
ALBUM_GROUP_ID = "album_develop_0715"
VIDEO_GROUP_ID = "video_develop_0715"

# 允许接入的媒资牌照及频道范围
AVAILABLE_DATA = {"album": {"310": ["少儿", "健身", "教育"],  # 雷咚咚(只有专辑数据)
                            "12": [],  # 芒果
                            "17": [],  # 小米
                            "19": [],  # 优酷
                            "11": [],  # 爱奇艺
                            "13": [],  # 腾讯未来
                            "15": [],  # 腾讯南传
                            "22": [],  # B站(全部频道)
                            "50": [],  # TVB(全部频道)
                            "55": [],  # 搜狐
                            "56": [],  # 新视听(全部频道)
                            "57": [],  # 大健康(全部频道)
                            "320": [],  # 欢喜传媒(全部频道)

                            },

                  "video": {
                      "12": [],
                      "17": [],
                      "19": [],
                      "11": [],  # 爱奇艺
                      "15": [],
                      "13": [],
                      "22": [],  # B站(全部频道)
                      "50": [],  # TVB(全部频道)
                      "55": [],  # 搜狐
                      "56": [],  # 新视听(全部频道)
                      "57": [],  # 大健康(全部频道)
                      "320": [],  # 欢喜传媒(全部频道)
                  }}
