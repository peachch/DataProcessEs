
import gc
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from collections import Iterator
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

import numpy
import pandas
import requests
from retry import retry
from tqdm import tqdm
import settings


class OssHelper(object):
    def __init__(self, url, key, key_secret, data_type):
        self.url = url
        self.key = key
        self.key_secret = key_secret
        self.data_type = data_type
        self.logger = logging
        self.path_dir = os.path.join(settings.BASE_DIR, "oss_download", data_type)

    @retry(tries=3)
    def get_download_urls(self, date):
        """获取下载链接"""
        payload = json.dumps({"date": date, "dataType": self.data_type})
        headers = {
            'Content-Type': "application/json",
            'key': self.key,
            'keySecret': self.key_secret,
        }
        resp_dict = requests.request("POST", url=self.url, data=payload, headers=headers).json()
        download_urls = resp_dict.get("result", [])
        if download_urls:
            self.logger.info(f"download url: {download_urls}")

        return download_urls

    @staticmethod
    def wget_fetch(download_url, path_dir):
        """调用wget下载数据"""
        filename = re.search(r"/parquet/(?P<filename>.+)\?", download_url).group("filename")
        save_path = os.path.join(path_dir, filename)

        cmd = f'wget -c --quiet --tries=3 --timeout=60 --output-document="{save_path}" "{download_url}"'
        cmd_list = shlex.split(cmd)
        # from python lib manual
        # Run the command described by args. Wait for command to complete, then return a CompletedProcess instance.
        cp = subprocess.run(cmd_list)
        if cp.returncode != 0:
            logging.error(f'download fail; url:{download_url}')
            raise Exception(f'download fail; url:{download_url}')

        # self.logger.debug(f"Download success; file: {save_path}")
        return save_path

    def start_download(self, date: str):
        # 获取下载链接
        download_completed_file = []
        download_urls = self.get_download_urls(date)
        if not download_urls:
            logging.info(f"file not uploaded, {self.data_type}, {date} ")
            return download_completed_file

        # 创建目录
        dir_path_date = os.path.join(self.path_dir, date)
        # try:
        #     shutil.rmtree(dir_path_date)    # 删除旧文件
        # except FileNotFoundError:
        #     pass
        os.makedirs(name=dir_path_date, exist_ok=True)
        lock = Lock()
        pbar = tqdm(total=len(download_urls), desc="downloading", ncols=100)

        def on_done(future):
            completed_file = future.result()
            if completed_file:
                with lock:
                    download_completed_file.append(completed_file)
                    pbar.update(1)  # 显示下载进度

        with ThreadPoolExecutor(max_workers=min(len(download_urls), 4)) as executor:
            for download_url in download_urls:
                f = executor.submit(self.wget_fetch, download_url=download_url, path_dir=dir_path_date)
                f.add_done_callback(on_done)
            executor.shutdown()
        pbar.close()

        # 删除本次下载以外的文件
        for filename in os.listdir(dir_path_date):
            filepath = os.path.join(dir_path_date, filename)
            if filepath not in download_completed_file:
                os.remove(filepath)
                logging.info(f"delete old file: {filepath}")

        # 校验文件有效性, 存在空文件的情况
        available_files = set()
        for filepath in download_completed_file:
            file_size = os.path.getsize(filepath)
            if file_size > 20 * 1024:   # 过滤小文件
                available_files.add(filepath)
            else:
                total = 0
                df = pandas.read_parquet(path=filepath)
                for _, row in df.iterrows():
                    total += 1
                    if total >= 10:
                        available_files.add(filepath)
                        break
                else:
                    os.remove(filepath)
                    logging.warning(f"delete empty file: {filepath}, size： {file_size}Byte")

        return list(available_files)

    @staticmethod
    def formatting_field(obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif pandas.isna(obj):
            return None
        else:
            return obj

    def read_parquet_date(self, date, query=None) -> Iterator:
        """读取当天的所有文件，并按文件返回结果"""
        dir_path_date = os.path.join(self.path_dir, date)
        filenames = [n for n in os.listdir(dir_path_date) if n.endswith("parquet")]
        filenames.sort()

        if not filenames:
            self.logger.error(f"not found {dir_path_date}")
            return

        for filename in tqdm(filenames, desc="reading", ncols=100):
            abs_path = os.path.join(dir_path_date, filename)
            df = pandas.read_parquet(path=abs_path)
            if query:
                df.query(query, inplace=True)  # 查询条件,只返回指定数据
            for _, row in df.iterrows():
                # 将array对象转换为列表,替换Nan
                data_formatted = {k: self.formatting_field(v) for k, v in row.items()}
                yield data_formatted
            del df
