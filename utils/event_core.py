from queue import Queue, Empty
from threading import *
from utils.utils_class import singleton


class Event(object):
    def __init__(self, event_type):
        """事件对象"""
        self.type_ = event_type  # 事件类型
        self.dict = {}  # 字典用于保存具体的事件数据


@singleton
class EventManager:
    def __init__(self):
        """初始化事件管理器"""
        self.__eventQueue = Queue()
        self.__active = False
        self.__thread = Thread(target=self.__Run)

        self.__handlers = {}  # {事件类型:[事件回调]}

    def __Run(self):
        """引擎运行"""
        while self.__active:
            try:
                event = self.__eventQueue.get(block=True, timeout=1)
                self.__EventProcess(event)
            except Empty:
                pass

    def __EventProcess(self, event):
        """处理事件"""
        if event.type_ in self.__handlers:
            for handler in self.__handlers[event.type_]:
                handler(event)

    def Start(self):
        """启动"""
        self.__active = True
        self.__thread.start()

    def Stop(self):
        """停止"""
        self.__active = False
        self.__thread.join()

    def AddEventListener(self, type_, handler):
        """绑定事件和监听器处理函数"""
        try:
            handlerList = self.__handlers[type_]
        except KeyError:
            handlerList = []

        self.__handlers[type_] = handlerList
        if handler not in handlerList:
            handlerList.append(handler)

    def RemoveEventListener(self, type_, handler):
        """移除监听器的处理函数"""
        pass

    def SendEvent(self, event):
        """发送事件，向事件队列中存入事件"""
        self.__eventQueue.put(event)
