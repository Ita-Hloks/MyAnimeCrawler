import asyncio
import os
from collections import Counter
import aiofiles
import aiohttp
import requests
import re
from lxml import etree

from funcs import try_to_get
# ATTENTION: config was put in gitignore
from config import URL, HEADERS, Episode_URL

def get_episode_list_url(url):
    """
    Get the every episode link, return a list
    Format
    episode_list = [[source1], [source2], length] - (length=2)
    reso_list = [episode1.link, episode2.link, length] - (length=2)
    """
    resp = requests.get(url, headers=HEADERS)
    content = etree.HTML(resp.text)
    episode_list = []  # Master List
    divs = content.xpath('//div[@class="anthology-list-box none"]/div/ul')
    for div in divs:
        lis = div.xpath('./li/a')
        reso_list = []  # The address of each video source, the last was the length
        for li in lis:
            episode_link = li.xpath('./@href')[0].split('/')[-1]
            episode_num = li.xpath('normalize-space(./text())')
            print(episode_num, ":", Episode_URL + episode_link)
            reso_list.append(Episode_URL)
        reso_list.append(len(reso_list))
        episode_list.append(reso_list)
        print("=============================================")
    print("[OK] All links have been successfully retrieved! Now attempting to download....")
    episode_list.append(len(episode_list))
    return episode_list


# Fetch m3u8 Url for Source code, return m3u8 download link
def get_episode_m3u8(url):
    resp = try_to_get(url, name="Index Link For M3U8", headers=HEADERS)
    obj_find_index_m3u8 = re.compile(r"https://dxfbk.com/\?url=(?P<index_m3u8_url>.*?)' title=", re.S)
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
    result = try_to_get(video_m3u8_url, name="M3U8 File", headers=HEADERS)
    save_address = address + "video.m3u8"
    with open(save_address, "wb") as f:
        f.write(result.content)
    print(f"[OK] .m3u8 File Download Successful, Save path: {save_address}")


async def download_ts(url, line, session):
    async with session.get(url, headers=HEADERS) as response:
        async with aiofiles.open(f'./m3u8/{line}', 'wb') as f:
            await f.write(await response.read())
    print(f"{line} Download Successful")


async def download_video(head_url):
    tasks = []
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with aiofiles.open("./m3u8/video.m3u8", "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                download_url = head_url + line
                task = asyncio.create_task(download_ts(download_url, line, session))
                tasks.append(task)
            await asyncio.wait(tasks)


def merge_files(m3u8_path="./m3u8/video.m3u8", output_file="./the_file.ts"):
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
    m3u8_head_url, video_m3u8_url = get_episode_m3u8(URL)  # Page source code URL
    # download_m3u8(video_m3u8_url, "./m3u8/")
    # asyncio.run(download_video(m3u8_head_url))
    # merge_files()
