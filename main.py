import asyncio
import os
import random
import string
import time
from urllib.parse import urljoin
import aiofiles
import aiohttp
import re
from lxml import etree
from pathlib import Path

from ad_filter_func import *
from funcs import try_to_get, w_sanitize, safe_remove_continue, menu_select
# ATTENTION: config was put in gitignore
from config import URL, HEADERS, Episode_URL

obj_find_anime_name = re.compile(r'<title>《(?P<name>.*?)》.*?</title>', re.S)
obj_find_index_m3u8 = re.compile(r"https://dxfbk.com/\?url=(.*?)' title=", re.S)

#  ======================= PARAMS =======================
HISTORY_PATH = "./m3u8/history.txt"


def get_episode_list_url(url: str):
    """
    download the list about the episode number, avoid possible request interruptions
    FORMAT:
    # episode_1
    example//1.com
    # episode_2
    example//2.com
    :param url: which source code includes the episode download link
"""
    resp = try_to_get(url, name="Home source code", headers=HEADERS)
    content = etree.HTML(resp.text)
    divs = content.xpath('//div[@class="anthology-list-box none"]/div/ul')
    source_list = content.xpath('//div[@class="anthology-tab nav-swiper b-b br"]/div/a/text()')

    match_name = obj_find_anime_name.search(resp.text)

    if match_name is None:
        # If fail to fetch anime name, Generate one at random
        anime_name = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        print("[WARN] Anime Name was not found")
    else:
        anime_name = w_sanitize(match_name.group('name'))
        # ensure it can be saved correctly
    print("[INFO] Request Anime Name: ", anime_name)

    path = f"./m3u8/{anime_name}/"

    #  Check the Path and father folder is existed? If not, create it
    Path(path).mkdir(parents=True, exist_ok=True)
    #  Delete the old file
    safe_remove_continue(f"{path}/cache/downloadList.txt")

    source_index = 0
    with open(f"{path}/cache/downloadList.txt", "a", encoding="utf-8") as f:
        f.write(str("-Video-Source: "))
        for source in source_list:
            source = w_sanitize(source)
            f.write("-" + source)
        f.write("\n")

        for div in divs:
            lis = div.xpath('./li/a')
            reso_list = []  # The address of each video source, the last was the length
            f.write(f"========= {w_sanitize(source_list[source_index])} =========\n")
            source_index += 1
            for li in lis:
                episode_link = li.xpath('./@href')[0].split('/')[-1]
                episode_num = li.xpath('normalize-space(./text())')
                # Write the episode_num and download link
                f.write(f"# {episode_num}\n")
                f.write(f"{Episode_URL + episode_link}\n")
                reso_list.append(Episode_URL)
    print("[OK] All links have been successfully retrieved! Now attempting to download....")
    return anime_name


def get_episode_m3u8(url: str):
    """
    Fetch m3u8 Url for Source code, return m3u8 download link
    :param url: which can get the ndex.m3u8 link
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

    print("[OK] Successfully Obtained the Genuine M3U8 Request Link, Start to Download M3U8 File...")
    return m3u8_head_url, video_m3u8_url


def download_m3u8(video_m3u8_url: str, address: str):
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


async def download_ts(url: str, filename: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
    try:
        async with sem:  # concurrency limit
            async with session.get(url, headers=HEADERS) as resp:
                resp.raise_for_status()
                content = await resp.read()

            os.makedirs('./m3u8', exist_ok=True)
            dest_path = os.path.join('./m3u8', filename)
            # ensure the parent path exist
            dest_dir = os.path.dirname(dest_path)
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)

            async with aiofiles.open(dest_path, 'wb') as f:
                await f.write(content)

        print(f"{filename} Successful")
        return True
    except Exception as e:
        print(f"{filename} Failed: {e}")
        return e  # return the abnormal data


async def download_video(head_url: str, path: str = None, pattern: str = "M",
                         tasks: list | None = None, concurrency: int = 15):
    """
    download m3u8 video concurrency

    :param head_url: segments (base url)
    :param path: m3u8 path (pattern == "M")
    :param pattern: "M": download m3u8 file, "T" according to the task list
    :param tasks: the task list which need download (pattern != "M")
    :param concurrency: concurrency request limit
    """
    names = []

    if pattern == "M":
        if not path:
            raise ValueError("[ERR] Must offer path (m3u8 file), When pattern == 'M'")
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            async for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                names.append(line)
    else:
        if tasks is None:
            raise ValueError("[ERR] Must offer tasks list, When pattern != 'M'")
        names = list(tasks)

    if not names:
        print("No segments to download.")
        return

    connector = aiohttp.TCPConnector(ssl=False)
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        download_tasks = []
        for name in names:
            if name.startswith("http://") or name.startswith("https://"):
                download_url = name
            else:
                download_url = urljoin(head_url, name)
            download_tasks.append(asyncio.create_task(download_ts(download_url, name, session, sem)))

        results = await asyncio.gather(*download_tasks, return_exceptions=True)

    failed = [r for r in results if isinstance(r, Exception)]
    if failed:
        print(f"{len(failed)} segments failed.")
    else:
        print("All segments downloaded successfully.")

    return results


def merge_m3u8(m3u8_path, output_file, auto_detect=True, manual_review=False):
    ts_list = []
    ad_list = []

    # Read m3u8 File
    with open(m3u8_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ts_list.append(line)

    print(f"The M3U8 File Contains {len(ts_list)} Fragment(s)")

    if not auto_detect:
        # don't auto analyze, merge file directly.
        print("[INFO] Auto-Detection Disabled, Merging All Segments...")
        filtered_list = ts_list
    else:
        # analyze the naming patterns
        main_pattern, patterns = ads_detect_analyze_ts_pattern(ts_list)
        print(f"[DBG] Naming Pattern Analysis: {patterns}")
        print(f"[DBG] Main Pattern: {main_pattern}")

        ad_indices = set()
        # according to the naming patterns to choice the analysis strategies
        if main_pattern == 'sequential':
            # Continuous number naming: using the sequence analysis
            print("[INFO] Using Sequence-Based AD Detection...")
            ad_indices = set(ads_detect_by_sequence(ts_list))

        elif main_pattern == 'md5_hash':
            # MD5-hash naming：using the duration analysis + file size analysis
            print("[INFO] Detected MD5-Hash Naming, Using Multi-Strategy Detection...")

            # Strategy1: duration analysis
            try:
                duration_ads = set(ads_detect_by_duration(ts_list, m3u8_path))
                print(f"[DBG] Duration-Based Detection: {len(duration_ads)} suspicious segments")
            except Exception as e:
                print(f"[WARN] Duration Analysis Failed: {e}")
                duration_ads = set()

            # Strategy2: Analyze file size
            try:
                size_ads = set(ads_detect__by_filesize(ts_list))
                print(f"[DBG] Size-Based Detection: {len(size_ads)} suspicious segments")
            except Exception as e:
                print(f"[WARN] Size Analysis Failed: {e}")
                size_ads = set()
            # Take the intersection (both methods consider labeling as AD)
            ad_indices = duration_ads & size_ads
            print(f"[INFO] Confirmed AD Segments (Intersection): {len(ad_indices)}")
            # If there is no intersection, it means the analysis not unreliable and no segments will be deleted
            if not ad_indices:
                print("[INFO] No Reliable AD Detection, Keeping All Segments")

        else:
            # other states: conservative strategy, not delete
            print("[INFO] Mixed/Unknown Naming Pattern, Skipping AD Detection")
            ad_indices = set()
        # Build a filtered list
        filtered_list = []
        for i, entry in enumerate(ts_list):
            if i in ad_indices:
                ad_list.append(os.path.basename(entry))
            else:
                # Remain m3u8 relative path
                candidate = entry
                if not os.path.isabs(candidate) and not candidate.startswith("http"):
                    candidate = os.path.join("./m3u8", os.path.basename(entry))
                filtered_list.append(candidate)

    if ad_list:
        print(f"\n[INFO] Identified {len(ad_list)} AD Segment(s) (Will Be Filtered):")
        # Only show the first 20 to avoid flooding the screen
        for ad in ad_list[:20]:
            print(f"  - {ad}")
        if len(ad_list) > 20:
            print(f"  ... and {len(ad_list) - 20} more")

        # if it needs the manual confirmation
        if manual_review:
            response = input("\n[?] Proceed with filtering? (y/n, default=y): ").strip().lower()
            if response == 'n':
                print("[INFO] Filtering Cancelled, Merging All Segments...")
                filtered_list = ts_list
    else:
        print("[INFO] No AD Segments Detected")

    print(f"\n[INFO] Will Merge {len(filtered_list)} Segment(s)...")

    # Merge ts Files
    try:
        with open(output_file, "wb") as outfile:
            for ts_file in filtered_list:
                if not os.path.exists(ts_file):
                    print(f"[WARN] File Not Exist - {ts_file}")
                    continue

                with open(ts_file, "rb") as infile:
                    outfile.write(infile.read())

        print(f"\n[SUCCESS] Output File: {os.path.abspath(output_file)}")
        print(f"[INFO] File Size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")

    except Exception as e:
        print(f"[ERR] Merge Failed: {e}")


def get_source_list(m3u8_path: str):
    source_list = []

    try:
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Check if it starts with # Video Source:
                if line.startswith("-Video-Source:"):
                    content = line.split(":", 1)[1].strip()
                    # according to '#' Split and clean each source
                    sources = [item.strip() for item in content.split("-") if item.strip()]
                    source_list.extend(sources)

    except FileNotFoundError:
        print(f"[ERR] File: {m3u8_path} not found")
    except Exception as e:
        print(f"[ERR] Read file fail {e}")

    return source_list


def choice_video_source(path, source_index):
    episode_number = []
    episode_link = []

    with open(path, "r", encoding="utf-8") as f:
        start_processing = False

        for line in f:
            line = line.strip()
            if line.startswith("="):
                if start_processing:
                    break
                source_index -= 1
                if source_index <= 0:
                    start_processing = True
                else:
                    start_processing = False
                    continue
            elif start_processing:
                if line.startswith("#"):
                    episode_number.append(line.strip("# "))
                elif line.startswith("-"):
                    continue
                else:
                    episode_link.append(line.strip(" "))
    return episode_number, episode_link


def retrieve_history_downloadList(url, search_path, check_history=True):
    if check_history is False:
        return "not found"

    history_path = Path(search_path)
    history_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure m3u8 folder exist

    if not history_path.exists():
        history_path.touch()

    with open(HISTORY_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith(f"{url}="):
                anime_name = line.split("=")[1]
                print(f"[OK] Obtain historical download records, get the name: {anime_name}")
                return anime_name
    print("[INFO] History DownloadList Not Found, Start to request...")
    return "not found"


def retrieve_history_m3u8(search_path, check_history=True):
    """
    TODO: Wait for use...
    """
    if check_history is False:
        return "not found"

    with open(search_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        if lines:  # Ensure the line isn't null
            last_line = lines[-1].strip()
            return '#EXT-X-ENDLIST' in last_line
    return False


def check_m3u8_files(path):
    """
    Check if all ts files in the m3u8 list exist
    :param path: m3u8 and ts file path
    :return: ts file name which don't exist (task_list) or "all files exist"
    """
    ts_list = []
    task_list = []

    # read m3u8 file
    with open(urljoin(path, "video.m3u8"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ts_list.append(line)

    # Check if each ts file exists
    for ts in ts_list:
        ts_path = os.path.join(path, ts)
        if not os.path.exists(ts_path):
            task_list.append(ts)

    if task_list:
        return task_list
    else:
        return "all files exist"


if __name__ == '__main__':
    Check_history = True
    is_new_anime = False
    anime_name = retrieve_history_downloadList(URL, HISTORY_PATH, check_history=True)

    if anime_name == "not found":
        is_new_anime = True
        anime_name = get_episode_list_url(URL)
        print("[INFO] Save this download request")
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(f"{URL}={anime_name}\n")

    DOWNLOAD_LIST_PATH = f"./m3u8/{anime_name}/downloadList.txt"

    #  Choice the download source
    source_list = get_source_list(DOWNLOAD_LIST_PATH)
    source_choice_name, source_choice_index = menu_select("Choice the download source", source_list)
    episode_number, episode_link = choice_video_source(DOWNLOAD_LIST_PATH, source_choice_index)

    print(episode_number)
    print(episode_link)
    download_video_index_start = int(input("Input the download start index: \n > "))
    download_video_index_end = int(input("Input the download end index: \n > "))

    for i in range(download_video_index_start - 1, download_video_index_end):
        path = f"./m3u8/{anime_name}/cache/{episode_number[i]}_{source_choice_name}/"

        if not episode_link:
            print("[ERR] No Episodes Found")
            break

        m3u8_head_url, video_m3u8_url = get_episode_m3u8(episode_link[i])  # Page source code URL
        download_m3u8(video_m3u8_url, path)

        if not is_new_anime:
            task_list = check_m3u8_files(path)
            if task_list != "all files exist":
                print(task_list)
                asyncio.run(download_video(m3u8_head_url, pattern="T", tasks=task_list))
        else:
            asyncio.run(download_video(m3u8_head_url, pattern="M", path=f"{path}video.m3u8"))
        print("[DBG] skip the merge, now over")
        merge_m3u8(f"{path + "video.m3u8"}",
                   f"./m3u8/{anime_name}/{anime_name + episode_number[i] + source_choice_name}.ts")
        print(f"{episode_number[i]} download successful! now sleep 5 second...")
        time.sleep(5)
    print("Mission Complete!")
