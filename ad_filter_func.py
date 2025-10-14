import os
import re
from collections import Counter


def analyze_ts_pattern(ts_list):
    """analyze the ts file naming pattern"""
    patterns = {
        'sequential': 0,  # Continuous number naming (0.ts, 1.ts, 2.ts...)
        'md5_hash': 0,  # MD5-hash naming (32bit-16 hexadecimal)
        'timestamp': 0,  # Timestamp naming (long digits)
        'mixed': 0  # Mixed naming
    }

    for entry in ts_list:
        base = os.path.basename(entry)
        name = os.path.splitext(base)[0]

        # Determine whether it is a pure number
        if name.isdigit():
            if len(name) <= 6:
                patterns['sequential'] += 1
            else:
                patterns['timestamp'] += 1
        # Determine whether it is MD5-hash
        elif re.match(r'^[a-f0-9]{32}$', name, re.IGNORECASE):
            patterns['md5_hash'] += 1
        else:
            patterns['mixed'] += 1

    # return the main pattern
    main_pattern = max(patterns, key=patterns.get)
    return main_pattern, patterns


def detect_ads_by_sequence(ts_list):
    """Analyze the AD based on Index continuity (Suitable for continuous numerical naming)"""
    ad_indices = []
    numbers = []

    for i, entry in enumerate(ts_list):
        base = os.path.basename(entry)
        m = re.search(r'(\d+)\.ts$', base)
        if m:
            numbers.append((i, int(m.group(1))))
        else:
            numbers.append((i, None))

    # Analyze jumps in numerical sequences
    valid_numbers = [(i, n) for i, n in numbers if n is not None]
    if len(valid_numbers) < 3:
        return ad_indices

    # Calculate deviation distribution
    diffs = []
    for j in range(1, len(valid_numbers)):
        diff = valid_numbers[j][1] - valid_numbers[j - 1][1]
        diffs.append(diff)

    if not diffs:
        return ad_indices

    # Find the most common step size
    diff_counter = Counter(diffs)
    common_diff, _ = diff_counter.most_common(1)[0]

    # mark segments with abnormal jump
    for j in range(1, len(valid_numbers)):
        diff = valid_numbers[j][1] - valid_numbers[j - 1][1]
        # if the jumps very large (exceed the 3 times), maybe the AD
        if common_diff > 0 and diff > common_diff * 3:
            # Mark all segments within this jump interval
            start_idx = valid_numbers[j - 1][0]
            end_idx = valid_numbers[j][0]
            ad_indices.extend(range(start_idx + 1, end_idx))

    return ad_indices


def detect_ads_by_duration(ts_list, m3u8_path):
    """analyze the AD based on duration (reading EXTINF tag form m3u8 file)"""
    ad_indices = []
    durations = []

    # Read the m3u8 file to get the duration of each segment
    with open(m3u8_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    current_duration = None
    ts_index = 0

    for line in lines:
        line = line.strip()
        # analyze #EXTINF tag
        if line.startswith('#EXTINF:'):
            match = re.search(r'#EXTINF:([\d.]+)', line)
            if match:
                current_duration = float(match.group(1))
        # encounter ts file
        elif line and not line.startswith('#'):
            if current_duration is not None:
                durations.append((ts_index, current_duration))
            ts_index += 1
            current_duration = None

    if len(durations) < 10:
        return ad_indices

        # Analyze duration distribution
    duration_values = [d for _, d in durations]
    duration_counter = Counter([round(d, 1) for d in duration_values])

    # find the most common duration
    if duration_counter:
        common_duration = duration_counter.most_common(1)[0][0]

        # Mark the abnormal duration file
        for idx, duration in durations:
            # the duration deviation exceeds 50%, it may be the AD segment
            if abs(duration - common_duration) > common_duration * 0.5:
                ad_indices.append(idx)

    return ad_indices


def detect_ads_by_filesize(ts_list):
    """Analyze the AD base on file size"""
    ad_indices = []
    sizes = []

    for i, entry in enumerate(ts_list):
        # Built file path
        candidate = entry
        if not os.path.isabs(candidate) and not candidate.startswith("http"):
            candidate = os.path.join("./m3u8", os.path.basename(entry))

        if os.path.exists(candidate):
            size = os.path.getsize(candidate)
            sizes.append((i, size))

    if len(sizes) < 10:
        return ad_indices

    # Calculate the median and standard deviation of file size
    size_values = [s for _, s in sizes]
    size_values.sort()
    median_size = size_values[len(size_values) // 2]

    # Calculate the absolute deviation
    mad = sum(abs(s - median_size) for s in size_values) / len(size_values)
    # mark segments with abnormal file size (too much deviation from median)
    for idx, size in sizes:
        # file size deviation exceeds 3 times MAD, it's maybe the AD
        if mad > 0 and abs(size - median_size) > 3 * mad:
            ad_indices.append(idx)

    return ad_indices
