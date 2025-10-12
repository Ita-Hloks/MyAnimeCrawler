import asyncio
import os
import random
import string
import time
from collections import Counter
import aiofiles
import aiohttp
import requests
import re
from lxml import etree
from pathlib import Path

from funcs import try_to_get, w_sanitize, safe_remove_continue
# ATTENTION: config was put in gitignore
from config import URL, HEADERS, Episode_URL

obj_find_anime_name = re.compile(r'<title>《(?P<name>.*?)》.*?</title>', re.S)
obj_find_index_m3u8 = re.compile(r"https://dxfbk.com/\?url=(.*?)' title=", re.S)


def get_episode_list_url(url):
    """
    download the list about the episode number, facilitating possible request interruptions
    FORMAT:
    # episode_1
    https://example//1.com
    # episode_2
    https://example//2.com
    ...
"""

    resp = requests.get(url, headers=HEADERS)
    content = etree.HTML(resp.text)
    divs = content.xpath('//div[@class="anthology-list-box none"]/div/ul')
    source_list = content.xpath('//div[@class="anthology-tab nav-swiper b-b br"]/div/a/text()')
    match_name = obj_find_anime_name.search(resp.text)

    if match_name is None:
        # If fail to fetch anime name, Generate one at random
        anime_name = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        print("[WARN] Anime Name was not found")
    else:
        anime_name = match_name.group('name')
        anime_name = w_sanitize(anime_name)
        # ensure it can be saved correctly
    print("[INFO] The anime name: ", anime_name)

    #  Check the Path and father folder is existed? If not, create it
    Path(f"./m3u8/{anime_name}").mkdir(parents=True, exist_ok=True)

    #  Delete the old file
    safe_remove_continue(f"./m3u8/{anime_name}/downloadList.txt")

    source_index = 0
    with open(f"./m3u8/{anime_name}/downloadList.txt", "a") as f:
        for div in divs:
            lis = div.xpath('./li/a')
            reso_list = []  # The address of each video source, the last was the length
            f.write(f"========= {w_sanitize(source_list[source_index])} =========\n")
            source_index += 1
            for li in lis:
                episode_link = li.xpath('./@href')[0].split('/')[-1]
                episode_num = li.xpath('normalize-space(./text())')

                print(episode_num, ":", Episode_URL + episode_link)
                # Write the episode_num and download link
                f.write(f"# {episode_num}\n")
                f.write(f"{Episode_URL + episode_link}\n")
                reso_list.append(Episode_URL)
            print("=============================================")
    print("[OK] All links have been successfully retrieved! Now attempting to download....")
    return anime_name


def get_episode_m3u8(url):
    """
    Fetch m3u8 Url for Source code, return m3u8 download link
"""
    resp = try_to_get(url, name="Index Link For M3U8", headers=HEADERS)

    m3u8_link = obj_find_index_m3u8.findall(resp.text)[0]
    print(f"[OK] Successfully Obtained The Index Link For M3U8: {m3u8_link}, Currently Concatenating URLs...")
    # 'https://???/20250708/19470_e0b22023/index.m3u8'
    resp_m3u8 = try_to_get(m3u8_link, name="M3U8 Request Link Suffix", headers=HEADERS)
    lines = resp_m3u8.content.splitlines()
    last_line = lines[-1].decode("utf-8")
    base_url = m3u8_link.rsplit("/", 1)[0] + "/"
    # 'https://???/20250708/19470_e0b22023/' Remove m3u8 tail

    video_m3u8_url = base_url + last_line
    m3u8_head_url = video_m3u8_url.rsplit("/", 1)[0] + "/"
    # 'https://???/20250708/19470_e0b22023/2000k/hls/' m3u8 Request URL

    print("[OK] Successfully Obtained the Genuine M3U8 Request Link, Start to Download M3U8 File")
    return m3u8_head_url, video_m3u8_url


def download_m3u8(video_m3u8_url, address):
    directory = os.path.dirname(address)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"[INFO] Create Folder: {directory}")

    result = try_to_get(video_m3u8_url, name="M3U8 File", headers=HEADERS)
    save_address = address + "video.m3u8"
    print(save_address)
    with open(save_address, "wb") as f:
        f.write(result.content)
    print(f"[OK] .m3u8 File Download Successful, Save path: {save_address}")


async def download_ts(url, line, session):
    async with session.get(url, headers=HEADERS) as response:
        async with aiofiles.open(f'./m3u8/{line}', 'wb') as f:
            await f.write(await response.read())
    print(f"{line} Download Successful")


async def download_video(head_url, path):
    """
    According to the m3u8 file, asynchronous download the .ts segments, which use the download_ts function.
"""
    tasks = []
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                download_url = head_url + line
                task = asyncio.create_task(download_ts(download_url, line, session))
                tasks.append(task)
            await asyncio.wait(tasks)


def merge_files(m3u8_path, output_file):
    """
    merge the segments .ts files to a complete m3u8 file.
"""
    ts_list = []
    ad_list = []

    # Read m3u8 File
    with open(m3u8_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line or line.startswith("#"):
                continue
            line = line.strip()
            ts_list.append(line)

    print(f"The M3U8 File Contains {len(ts_list)} Fragment")

    # Extract the length of the numeric suffix from each filename
    digit_len_counts = Counter()
    basename_to_digits = {}
    for entry in ts_list:
        base = os.path.basename(entry)
        m = re.search(r'(\d+)\.ts$', base)
        if m:
            digits = m.group(1)
            basename_to_digits[entry] = digits
            digit_len_counts[len(digits)] += 1
        else:
            basename_to_digits[entry] = ""

        # Find the normal number length of segments
    if digit_len_counts:
        common_len, _ = digit_len_counts.most_common(1)[0]
    else:
        common_len = None

    print(f"[DBG] Numerical length distribution: {dict(digit_len_counts)} -> Normal Length: {common_len}")

    # Determine ad based on abnormal numerical length (as long as the length isn't equal to most common length)
    filtered_list = []
    for entry in ts_list:
        digits = basename_to_digits.get(entry, "")
        if digits and common_len is not None and len(digits) != common_len:
            ad_list.append(os.path.basename(entry))
        else:
            # Retain m3u8 relative path
            candidate = entry
            if not os.path.isabs(candidate) and not candidate.startswith("http"):
                candidate = os.path.join("./m3u8", os.path.basename(entry))
            filtered_list.append(candidate)

    if ad_list:
        print(f"[INFO] Identify {len(ad_list)} AD Segment(Was Filtered):")
        for ad in ad_list:
            print(f"  - {ad}")
    else:
        print("[INFO] No AD Segments Detected")

    print(f"\n[INFO] Retain {len(filtered_list)} Right Segments, Try To Merge...")

    # Merge ts Files
    try:
        with open(output_file, "wb") as outfile:
            for ts_file in filtered_list:
                if not os.path.exists(ts_file):
                    print(f"[WARN] File Not Exist - {ts_file}")
                    continue

                with open(ts_file, "rb") as infile:
                    outfile.write(infile.read())

        print(f"[INFO] Output File: {os.path.abspath(output_file)}")
        print(f"[INFO] File Size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")

    except Exception as e:
        print(f"[ERR] Merge Failed: {e}")


if __name__ == '__main__':
    # anime_name = get_episode_list_url(URL)
    anime_name = "我们不可能成为恋人！绝对不行。（※似乎可行？）"
    download_video_index_start = 1
    download_video_index_end = 2

    episode_link = []
    episode_number = []

    source_index = 1

    with open(f"./m3u8/{anime_name}/downloadList.txt", "r") as f:
        for line in f:
            #  TODO: support to switch the download source.
            line = line.strip()
            if line.startswith("="):
                source_index -= 1
                if source_index <= 0:
                    continue  # skip
                else:
                    break
            elif line.startswith("#"):
                episode_number.append(line.strip("# "))
            else:
                episode_link.append(line.strip(" "))
    for i in range(download_video_index_start - 1, download_video_index_end):
        path = f"./m3u8/{anime_name}/{episode_number[i]}/"
        m3u8_head_url, video_m3u8_url = get_episode_m3u8(episode_link[i])  # Page source code URL
        download_m3u8(video_m3u8_url, path)
        asyncio.run(download_video(m3u8_head_url, path + "video.m3u8"))
        merge_files(f"{path + "video.m3u8"}", f"./m3u8/{anime_name}/{anime_name + episode_number[i]}.ts")
        time.sleep(5)
