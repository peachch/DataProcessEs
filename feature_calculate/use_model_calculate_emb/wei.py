# encoding=utf-8

from sum_media_script.sum_media_info import extract
import json
from tqdm import tqdm
import codecs
import sys

def get_keywords(data_json_file, save_file,sim_dict_file):
    with codecs.open(sim_dict_file, 'r', encoding='utf-8') as f:
        sim_dict = json.load(f)

    with open(data_json_file, 'r') as f, codecs.open(save_file, 'a', encoding='utf-8') as w:
        for line in tqdm(f.read().split('\n')[:-1]):
            keyword_dict = extract(json.loads(line))
            sim_keywords = []
            for word in keyword_dict['keywords']:
                if word in sim_dict:
                    sim_keywords.extend(sim_dict[word][:3])

            keyword_dict.pop('keywords')
            keyword_dict['sim_keywords'] = sim_keywords

            w.write(json.dumps(keyword_dict, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    data_file = sys.argv[1]
    get_keywords(data_json_file = data_file)
