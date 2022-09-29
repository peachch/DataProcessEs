import difflib
import requests
from flask import Flask, request
from retry import retry

app = Flask(__name__)


@retry(tries=3)
def fetch(url):
    resp_dict = requests.get(url).json()
    print(resp_dict, url)
    doc_content = resp_dict.get("_source", {})
    return doc_content


@app.route('/')
def hello_world():
    return 'Hello, World!'


def resort_media_dict(media_dict: dict):
    for key, value in media_dict.items():
        if isinstance(value, list):
            value.sort()

    resorted_key = list(media_dict.keys())
    resorted_key.sort()
    resorted_media = {k: media_dict[k] for k in resorted_key}
    return resorted_media


def extract_diff(dict1: dict, dict2: dict):
    d = difflib.HtmlDiff()
    html_diff = d.make_file([f"{k}:{v}" for k, v in dict1.items()],
                            [f"{k}:{v}" for k, v in dict2.items()])

    return html_diff


@app.route('/compare_index', methods=["GET", ])
def compare():
    index_name = request.args.get("index")
    data_id = request.args.get("id")
    print(index_name, data_id)
    media_test = fetch(url=f"http://ip:8710/{index_name}/_doc/{data_id}")
    media_preprod = fetch(url=f"http://ip:8710/{index_name}/_doc/{data_id}")

    media_test = resort_media_dict(media_test)
    media_test["env"] = "RC环境"
    media_preprod = resort_media_dict(media_preprod)
    media_preprod["env"] = "生产环境"
    print(media_test)
    print(media_preprod)
    # 重新排序

    html_diff = extract_diff(media_test, media_preprod)
    return html_diff


if __name__ == '__main__':
    app.run(port=port, host='0.0.0.0')
