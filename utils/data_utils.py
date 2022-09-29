import hashlib
from math import ceil

from tqdm import tqdm
import json
import re


class StrClean(object):
    def if_brackets_in_str(self, str):  # 去除括号
        if ')' in str or '）' in str:
            str = str.replace(")", "")
            str = str.replace("）", "")
        if '(' in str or '（' in str:
            pattern1 = re.compile("[\(（].*")
            find_pattern1 = re.findall(pattern1, str)

            if find_pattern1:
                str = self.delete_re_result(find_pattern1, str)
        return str

    @staticmethod
    def delete_re_result(list, str):
        for i in list:
            str = str.replace(i, "")
        return str

    def clean_sign(self, string):  # 去除所有符号只留英文数字和中文
        cleaned_string = ""
        for char in string:
            if '\u4e00' <= char <= '\u9fff' or char in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
                                                        "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
                                                        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
                                                        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y",
                                                        "Z"] or char in ["1", "2", "3", "4", "5", "6", "7", "8", "9",
                                                                         "0"] or char == " ":  # 要么是中文,要么是英文,要么是数字
                cleaned_string = cleaned_string + char
            else:
                continue
        cleaned_string.strip(" ")
        return cleaned_string

    def if_str_is_binary_code(self, str):  # 去除二进制数据
        pattern = re.compile("([A-Za-z][0-9])")
        find_pattern = re.findall(pattern, str)
        if len(find_pattern) >= 3:
            return ""
        else:
            return str

    def if_invaild_words(self, str):  # 去除不合法词
        invaild_list = ["暂无", "不详", "无", "无内容提供商", "其他", "其他地区", "未知", "剧场", "专区"]
        for i in invaild_list:
            str = str.replace(i, "")
        return str

    def if_only_need_chinese(self, str):  # 过滤所有符号只要中文
        cleaned_string = ""
        for char in str:
            if '\u4e00' <= char <= '\u9fff':
                cleaned_string = cleaned_string + char
            else:
                continue
        cleaned_string.strip(" ")
        return cleaned_string

    def if_invaild_number(self, str):  # 去除数字
        try:
            int(str)
        except ValueError:
            return str
        else:
            return ""

    def if_invaild_one_lence(self, str):  # 去除长度为1的数据
        if len(str) == 1:
            return ""
        else:
            return str

    def if_null_in_list(self, field_list):
        while "" in field_list:
            field_list.remove("")
        return field_list


class SplitStr(object):

    def split_str(self, stra):
        stra.strip()
        split_sign_list = ["、", "/", "\\", ",", "，", ";", "；", "|", " ", "string"]
        stra_list = [stra]
        for sign in split_sign_list:
            if sign in stra:
                if sign == " ":
                    stra_list = self.check_english_and_chinese(stra)
                else:
                    stra_list = stra.split(sign)
        return stra_list

    def check_english_and_chinese(self, stra):
        stra_list = stra.split(' ')
        while '' in stra_list:
            stra_list.remove('')
        chinese_list = list(filter(lambda x: ord(x[0]) > 127, stra_list))
        english_list = list(filter(lambda x: ord(x[0]) <= 127, stra_list))
        new_english_list = []
        english = ' '.join(english_list)
        new_english_list.append(english)
        new_stra_list = chinese_list + new_english_list
        try:
            new_stra_list.remove('')
        except ValueError:
            pass
        return new_stra_list


# 以不同方式创建字典的键,用于不同数据的筛查比较
class CreateDic(object):

    def combine_item_list_func(self, line, id_list, item_dic):
        id_only = ""
        for item in id_list:
            item_index = item_dic[item]
            id_only = str(line[item_index]) + id_only
        return id_only

    def combine_item_dic_func(self, dic, id_list):
        id_only = ""
        for item in id_list:
            value = dic[item]
            id_only = str(value) + id_only
        return id_only

    def combine_item_dic_and_sign_func(self, value, index):
        sign_dic = {"0": 'A', "1": 'B', "2": 'C', "3": 'D', "4": 'E', "5": 'F', "6": 'G', "7": 'H', "8": 'I', "9": 'J'}
        sign = sign_dic[str(index)[-1]]
        id_only = value + sign
        return id_only


class TitleCombine(object):

    @staticmethod
    def formatting_title_digest(title: str):
        """
        通过媒资标题合并时，预生成的标题摘要, 用于对比标注数据

        ！！！ 非常重要,更新时需要同步更新标注数据
        """
        title = title.replace("预告片", '')
        title = title.replace("预告", '')
        title = title.replace("花絮", '')
        title = title.replace("片花", '')
        title = title.replace("精彩片段", '')
        title = title.replace("曝光", '')
        title = title.replace("终极", '')
        title = title.replace("先导", '')
        title = title.replace("版", '')
        title = title.replace("提档", '')

        # 只保留中英文和数字
        title = re.sub(r"[^\u4e00-\u9fa5^.^a-z^A-Z^0-9]", "", title)

        # 将中文数字转换为阿拉伯数字
        def repl_ch_num(match):
            ch_num = match.group()
            a_num = chinese2num(ch_num)
            return str(a_num)

        title = re.sub(r"[零一二三四五六七八九十百千万]+", repl_ch_num, title)
        title_digest = hashlib.md5(title.encode()).hexdigest()

        return title_digest

    def title_combine(self, line_json, combine_list, compare_list, old_media_value, source):  # title及其他字段比较后符合一致规则数据融合
        line_json["statistic_item"] = [{"org": line_json["tags"]}]  # 新增一个用于统计标签的字段

        try:
            channel_flag = self.channel_check(line_json["channel"], old_media_value["channel"])
        except KeyError:
            line_json = self.combine_dic(line_json, old_media_value, combine_list, source)
        else:
            if channel_flag == "Ture":
                pass
            else:
                if len(line_json["title"]) >= 5:  # 直接合并
                    line_json = self.combine_dic(line_json, old_media_value, combine_list, source)
                else:
                    flag = self.rules1(line_json, old_media_value, compare_list)
                    if flag == "Ture":
                        line_json = self.combine_dic(line_json, old_media_value, combine_list, source)
        return line_json

    @staticmethod
    def rules1(line_json, old_media_value, compare_list):
        list_new = []
        list_old = []
        for field in compare_list:
            new_field_value = line_json[field]
            old_field_value = old_media_value[field]
            if isinstance(new_field_value, str):
                list_new.append(new_field_value)
            if isinstance(new_field_value, list):
                list_new = list_new + new_field_value
            if isinstance(old_field_value, str):
                list_old.append(old_field_value)
            if isinstance(old_field_value, list):
                list_old = list_old + old_field_value
        for data in list_new:
            if data in list_old:
                return "Ture"

    @staticmethod
    def channel_check(media_channel, old_media_channel):
        if media_channel in ["电影", "电视剧", "动漫"] and old_media_channel in ["综艺", "纪录片"]:
            return "Ture"
        if media_channel in ["综艺", "纪录片"] and old_media_channel in ["电影", "电视剧", "动漫"]:
            return "Ture"

    @staticmethod
    def combine_dic(line_json, old_media_value, combine_list, source):
        for field in combine_list:
            try:
                mline_field = line_json[field]
            except KeyError:
                mline_field = []
            oldline_field = old_media_value[field]
            while "NULL" in oldline_field:
                oldline_field.remove("NULL")
            field_list = list(set(mline_field + oldline_field))
            line_json[field] = field_list
            if field == "tags":
                source_index = {}
                for index, source_dic in enumerate(line_json["statistic_item"]):
                    source_index[list(source_dic.keys())[0]] = index
                if source not in source_index:
                    line_json["statistic_item"].append({source: oldline_field})
                else:
                    source_dic = line_json["statistic_item"][source_index[source]]
                    source_dic[source] = list(set(source_dic[source] + oldline_field))
                    line_json["statistic_item"][source_index[source]] = source_dic
        return line_json


CN_NUM = {
    '〇': 0,
    '一': 1,
    '二': 2,
    '三': 3,
    '四': 4,
    '五': 5,
    '六': 6,
    '七': 7,
    '八': 8,
    '九': 9,

    '零': 0,
    '壹': 1,
    '贰': 2,
    '叁': 3,
    '肆': 4,
    '伍': 5,
    '陆': 6,
    '柒': 7,
    '捌': 8,
    '玖': 9,

    '貮': 2,
    '两': 2,
}
CN_UNIT = {
    '十': 10,
    '拾': 10,
    '百': 100,
    '佰': 100,
    '千': 1000,
    '仟': 1000,
    '万': 10000,
    '萬': 10000,
    '亿': 100000000,
    '億': 100000000,
    '兆': 1000000000000,
}


def chinese2num(cn):
    try:
        ret = int(cn)
    except ValueError:
        lcn = list(cn)
        unit = 0  # 当前的单位
        ldig = []  # 临时数组

        while lcn:
            cndig = lcn.pop()

            if cndig in CN_UNIT:
                unit = CN_UNIT.get(cndig)
                if unit == 10000:
                    ldig.append('w')  # 标示万位
                    unit = 1
                elif unit == 100000000:
                    ldig.append('y')  # 标示亿位
                    unit = 1
                elif unit == 1000000000000:  # 标示兆位
                    ldig.append('z')
                    unit = 1

                continue

            else:
                dig = CN_NUM.get(cndig)

                if unit:
                    dig = dig * unit
                    unit = 0

                ldig.append(dig)

        if unit == 10:  # 处理10-19的数字
            ldig.append(10)

        ret = 0
        tmp = 0

        while ldig:
            x = ldig.pop()

            if x == 'w':
                tmp *= 10000
                ret += tmp
                tmp = 0

            elif x == 'y':
                tmp *= 100000000
                ret += tmp
                tmp = 0

            elif x == 'z':
                tmp *= 1000000000000
                ret += tmp
                tmp = 0

            else:
                tmp += x

        ret += tmp
        return ret
    else:
        return ret


class Create_dic(object):

    def combine_item_list_func(self, line, id_list, item_dic):
        id_only = ""
        for item in id_list:
            item_index = item_dic[item]
            id_only = str(line[item_index]) + id_only
        return id_only

    def combine_item_dic_func(self, dic, id_list):
        id_only = ""
        for item in id_list:
            value = dic[item]
            value = dic[item]
            id_only = str(value) + id_only
        return id_only

    def combine_item_dic_and_sign_func(self, value, index):
        sign_dic = {"0": 'A', "1": 'B', "2": 'C', "3": 'D', "4": 'E', "5": 'F', "6": 'G', "7": 'H', "8": 'I', "9": 'J'}
        sign = sign_dic[str(index)[-1]]
        id_only = value + sign
        return id_only


def load_data_dic(url):
    c = Create_dic()
    f = open(url, 'r', encoding='utf-8')
    print(url)
    new_dic = {}
    try:
        with tqdm(f) as f:
            for index, line in enumerate(f):
                line_json = json.loads(line.strip())
                if len(line_json) == 1:  # 有些渠道的数据有id的唯一标识
                    value_dic = line_json[list(line_json.keys())[0]]
                else:
                    value_dic = line_json
                title = value_dic["title"]
                if isinstance(title, list):
                    title = title[0]
                id_only = c.combine_item_dic_and_sign_func(title, index)
                if id_only in new_dic:
                    tags_list_new = value_dic["tags"]
                    exist_tags_list = new_dic[id_only]["tags"]
                    tags_list = list(set(exist_tags_list + tags_list_new))
                    new_dic[id_only]["tags"] = tags_list
                    new_dic[id_only] = value_dic
                else:
                    new_dic[id_only] = value_dic
    except KeyboardInterrupt:
        raise
    f.close()
    return new_dic


def load_file(url):
    f = open(url, 'r', encoding='utf-8')
    dic1 = {}
    for line in f:
        line = line.strip()
        dic1[line] = "null"
    f.close()
    return dic1


def cut_list(any_list, cut_num):
    """等分列表"""
    step = ceil(len(any_list) / cut_num)
    for idx in range(0, len(any_list), step):
        yield any_list[idx: idx + step]


if __name__ == '__main__':
    l = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, "a", "b"]
    s = 2
    for i in cut_list(l, s):
        print(i)
