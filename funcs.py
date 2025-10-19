import os
import re
import time

import requests
from bs4 import BeautifulSoup

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
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            print(f"[WARN] {name} get failed, Count down: {chance - attempt}")
            if attempt < chance - 1:
                print(f"[INFO] Wait {sleep} seconds and retry...")
                time.sleep(sleep)
            else:
                print("[ERR] Request failed, exit program")

    print(f"[BREAK] {name} request failed, already tried {chance} times")


def w_sanitize(name: str) -> str:
    """
    Perform secure processing on the name, ensure it can be saved correctly in Windows Explorer.
      - Remove HTML tags
      - Convert HTML spaces to normal spaces
      - Remove non-breaking spaces and other invisible characters
      - Replace Windows forbidden characters with '_'
      - Handle Windows reserved names
      - Strip leading/trailing spaces and dots
      - Collapse multiple spaces into one
"""
    if not name:
        return "_"

    # 1. Remove HTML tags
    name = BeautifulSoup(name, "html.parser").get_text()
    # 2. Convert HTML space &nbsp; and non-breaking space \xa0 to normal space
    name = name.replace("&nbsp;", " ").replace("\xa0", " ")
    # 3. Remove invisible/control characters (like \t, \n)
    name = re.sub(r'[\r\n\t]', '', name)
    # 4. Replace Windows forbidden characters: < > : " / \ | ? *
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # 5. Collapse multiple spaces into one
    name = re.sub(r'\s+', ' ', name)
    # 6. Strip leading/trailing spaces and dots
    name = name.strip(' .')
    # 7. Handling Windows reserved names
    reserved_names = {"CON", "PRN", "AUX", "NUL",
                      *{f"COM{i}" for i in range(1, 10)},
                      *{f"LPT{i}" for i in range(1, 10)}}
    if name.upper() in reserved_names:
        name = "_" + name
    # 8. Ensure non-empty name
    if not name:
        name = "_"

    return name


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


def menu_select(title, options):
    print(f"\n=== {title} ===")
    for i, opt in enumerate(options, 1):
        print(f"{i}. {opt}")
    while True:
        try:
            sel = int(input("Please select an option:\n > "))
            if 1 <= sel <= len(options):
                print(f"[OK] You Choice {options[sel - 1]}")
                return options[sel - 1], sel
        except ValueError:
            pass
        print("[WARN] Invalid selection]")
