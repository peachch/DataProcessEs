import psutil
import requests


class LazyProperty(object):
    """
    减少property装饰函数查询负载
    """

    def __init__(self, func):
        self._func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self

        else:
            value = self._func(instance)
            setattr(instance, self._func.__name__, value)
            return value


def singleton(cls):
    instance = {}

    def inner(*args, **kwargs):
        if cls not in instance:
            instance[cls] = cls(*args, **kwargs)
        return instance[cls]

    return inner


def get_pid(name):
    """根据进程名获取进程id"""
    pids = psutil.process_iter()
    for pid in pids:
        if pid.name() == name:
            pid_num = pid.pid
            return pid_num
    else:
        return None


def test_net():
    """测试网络连接"""
    print(r"""
      █████▒█    ██  ▄████▄   ██ ▄█▀       ██████╗ ██╗   ██╗ ██████╗
    ▓██   ▒ ██  ▓██▒▒██▀ ▀█   ██▄█▒        ██╔══██╗██║   ██║██╔════╝
    ▒████ ░▓██  ▒██░▒▓█    ▄ ▓███▄░        ██████╔╝██║   ██║██║  ███╗
    ░▓█▒  ░▓▓█  ░██░▒▓▓▄ ▄██▒▓██ █▄        ██╔══██╗██║   ██║██║   ██║
    ░▒█░   ▒▒█████▓ ▒ ▓███▀ ░▒██▒ █▄       ██████╔╝╚██████╔╝╚██████╔╝
     ▒ ░   ░▒▓▒ ▒ ▒ ░ ░▒ ▒  ░▒ ▒▒ ▓▒       ╚═════╝  ╚═════╝  ╚═════╝
     ░     ░░▒░ ░ ░   ░  ▒   ░ ░▒ ▒░
     ░ ░    ░░░ ░ ░ ░        ░ ░░ ░
              ░     ░ ░      ░  ░
                    ░               
    """)
    url = "https://xxxx"
    headers = {'Referer': "https://", }
    try:
        requests.packages.urllib3.disable_warnings()
        resp = requests.get(url, headers=headers, verify=False, timeout=3).json()["text"]
    except Exception:
        print("请检查公网连接！")
    else:
        print(resp.strip())
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
