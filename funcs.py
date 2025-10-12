import os
import re
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


def w_sanitize(name):
    """Perform secure processing on the name, ensure it can be saved correctly in Windows Explorer."""
    # remove HTML space
    name = name.replace("&nbsp;", " ")
    # Remove Windows disabled characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name).strip()
    # Handling Windows Reserved Names
    if name.upper() in {"CON", "PRN", "AUX", "NUL", *{f"COM{i}" for i in range(1, 10)},
                        *{f"LPT{i}" for i in range(1, 10)}}:
        name = "_" + name
    # Avoid spaces or full stops on the end
    return name.rstrip(' .')


def safe_remove_continue(file_path):
    """Safe remove the file, even if it meets Warning or Error"""
    try:
        os.remove(file_path)
        print(f"[OK] {file_path} has been removed")
    except FileNotFoundError:
        print(f"[LOG] {file_path} not exist")
    except PermissionError:
        print(f"[WARN] Not permission to delete {file_path}")
    except Exception as e:
        print(f"[ERR] Error occurred when deleting {file_path} :\n {e}")
