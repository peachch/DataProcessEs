import jieba as j
import json
import codecs as co
from collections import Counter
import math
import numpy as np

PATH = 'sum_media_script/'

j.load_userdict(PATH + 'zh_custom_dict.data')

STOP_WORDS = []
STOP_WORDS_MEDIA = []
MEDIA_HIGH_FREQ_WORDS = {}
COMMON_HIGH_FREQ_WORDS = {}

swlines = co.open(PATH + 'stopwords.txt', 'r', 'utf-8').readlines()
for swl in swlines:
    swl = swl.strip()
    if (swl != ''):
        STOP_WORDS.append(swl)

mhfwlines = co.open(PATH + 'high_freq_words_media', 'r', 'utf-8').readlines()
for mhfwl in mhfwlines:
    mhfwl = mhfwl.strip()
    if (mhfwl != ''):
        mhfwl = mhfwl.split('=>')
        if (len(mhfwl) != 2):
            print('error: ' + '//'.join(mhfwl))
        freq = int(mhfwl[0])
        words = mhfwl[1].split('>>')
        for word in words:
            MEDIA_HIGH_FREQ_WORDS[word] = freq

swflines = co.open(PATH + 'media_stop_words.txt', 'r', 'utf-8').readlines()
for swfl in swflines:
    swfl = swfl.strip()
    if (swfl != ''):
        STOP_WORDS_MEDIA.append(swfl)

chfwlines = co.open(PATH + 'wfreq-jieba-userdict.data', 'r', 'utf-8').readlines()
for chfwl in chfwlines:
    chfwl = chfwl.strip()
    if (chfwl != ''):
        chfwl = chfwl.split('/FREQ/')
        if (len(chfwl) == 2):
            COMMON_HIGH_FREQ_WORDS[chfwl[0]] = int(chfwl[1])
    # print(COMMON_HIGH_FREQ_WORDS)

print('resources load over...')

def drop_stop_word(words, table):
    nw = []
    for w in words:
        if (w not in table and len(w) > 1):
            nw.append(w)
    return nw

def drop_alpha_digit(words):
    nw = []
    for w in words:
        asciiw = w.encode('utf-8')
        if (not asciiw.isalpha() and not asciiw.isdigit()):
            nw.append(w)
    return nw

def print_counter(ct):
    if (len(ct) > 1):
        ks = list(ct.keys())
        r = u''
        for k in ks:
            r += k + '=' + str(ct[k]) + '    '
        print(r)

def get_common_freq_min(words):
    if (len(words) < 2):
        return words
    freqs = []
    for w in words:
        if w in COMMON_HIGH_FREQ_WORDS:
            freqs.append(COMMON_HIGH_FREQ_WORDS[w])
        else:
            freqs.append(300)
    # print("合并的常用词汇",freqs)
    st = np.argsort(freqs).tolist()
    # print("排序后的词汇",st)
    lst = len(st)
    # print("当前在计算的词汇",words)
    if (lst <= 3):
        return [words[st[0]]]
    elif (lst <= 5):
        return [words[st[0]], words[st[1]]]
    else:
        return [words[st[0]], words[st[1]], words[st[2]]]

def summ(words):
    if (len(words) < 2):
        return words
    words = drop_stop_word(words, table=STOP_WORDS)
    words = drop_stop_word(words, table=STOP_WORDS_MEDIA)
    words = drop_alpha_digit(words)
    counter = Counter(words)
    if (len(counter) == 0):
        return []
    # print_counter(counter)
    vs = list(counter.values())
    if (len(set(vs)) == 1):
        ks = list(counter.keys())
        cfq = []
        for k in ks:
            if k in COMMON_HIGH_FREQ_WORDS:
                cfq.append(COMMON_HIGH_FREQ_WORDS[k])
            else:
                cfq.append(300)
        cmin = min(cfq)
        cindex = cfq.index(cmin)
        return [ks[cindex]]
    rcount = 1
    if (len(vs) == 2):
        rcount = 1
    elif (len(vs) >= 3 and len(vs) <= 5):
        rcount = 2
    elif (len(vs) >= 6):
        rcount = 3
    # rcount = int(round(1.2 ** math.log(len(counter))))
    # if (rcount < 1):
    #     rcount = 1
    mc = counter.most_common(rcount)
    nw = []
    for _ in mc:
        '''
        if (_[1] < 2):
            k = _[0]
            if (MEDIA_HIGH_FREQ_WORDS.has_key(k)):
                if (MEDIA_HIGH_FREQ_WORDS[k] < 2500):
                    nw.append(k)
            else:
                nw.append(k)
        else:
            nw.append(_[0])
        '''
        nw.append(_[0])

    return nw


def extract(single_data_dict):
    words = []
    words += list(j.cut(single_data_dict['title']))
    words += list(j.cut(single_data_dict['brief']))
    for al in single_data_dict['aliases']:
        words += list(j.cut(al))
    sm = summ(words)

    ctags = drop_stop_word(drop_stop_word(single_data_dict['tags'], table=STOP_WORDS), table=STOP_WORDS_MEDIA)
    sm += get_common_freq_min(ctags)
    # print("最终单词keyword的返回",sm)
    if sm == [""]:
        print(sm)
    return {'aid': single_data_dict['aid'],
            'vendor': single_data_dict['vendor'],
            'keywords': sm}



if __name__ == '__main__':
    datas = co.open('combined_duan_zheng_old_keyword_query', 'r', 'utf-8').readlines()[:]
    res = []
    index = 0
    for data in datas:
        data = data.strip()
        data = data.lower()
        if (data != ''):
            if (index > 0 and index % 2000 == 0):
                print(str(index / float(len(datas)) * 100) + '%')
            try:
                index += 1
                jdata = json.loads(data)
            except Exception:
                continue
            words = []
            words += list(j.cut(jdata['title']))
            words += list(j.cut(jdata['brief']))
            # words += jdata['tags']
            # words += jdata['characters']
            for al in jdata['aliases']:
                words += list(j.cut(al))
            sm = summ(words)
            # if (len(sm) == 0):
            #     sm = drop_stop_word(list(j.cut(jdata['title'])), table=STOP_WORDS)
            # if (len(sm) == 0):
            #     sm = drop_stop_word(jdata['tags'], table=STOP_WORDS)
            ctags = drop_stop_word(drop_stop_word(jdata['tags'], table=STOP_WORDS), table=STOP_WORDS_MEDIA)
            sm += get_common_freq_min(ctags)
            # cchars = drop_stop_word(drop_stop_word(jdata['characters'], table=STOP_WORDS), table=STOP_WORDS_MEDIA)
            # sm += get_common_freq_min(cchars)
            res.append('ID:' + jdata['aid'] + '-' + jdata['vendor'] + '\n')
            res.append('SUM:' + '##'.join(sm) + '\n')
            res.append('DATA:' + jdata['title'] + '<//>' + '@@'.join(jdata['aliases']) + '<//>' + '@'.join(jdata['tags']) +\
                '<//>' + '@'.join(jdata['characters']) + '<//>' + jdata['brief'] + '\n\n')
    wf = co.open('re_sum_nochars.txt', 'w', 'utf-8')
    wf.writelines(res)
    wf.close()

