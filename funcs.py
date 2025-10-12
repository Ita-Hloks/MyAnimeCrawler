import time

import requests

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
}


def try_to_get(url, sleep=None, name=None, chance=None, headers=None):
    """Try to multiple requests. And return corresponding prompts"""

    if headers is None:
        headers = DEFAULT_HEADERS
    if chance is None:
        chance = 3
    if sleep is None:
        sleep = 3
    if name is None:
        name = url

    for attempt in range(chance):
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            print(f"[WARN] {name} get failed, Count: {attempt + 1}")
            if attempt < chance - 1:
                print(f"[INFO] Wait {sleep} seconds and retry...")
                time.sleep(sleep)
            else:
                print("[ERR] Request failed, exit program")

    print(f"[BREAK] {name} request failed, already tried {chance} times")