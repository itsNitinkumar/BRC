import math
import mmap
import multiprocessing
from collections import defaultdict
import os

def round_inf(x):
    return math.ceil(x * 10) / 10  

def default_city_data():
    return [float('inf'), float('-inf'), 0.0, 0]  # [min, max, total, count]

def process_chunk(filename, start_offset, end_offset):
    data = defaultdict(default_city_data)
    
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)

        if start_offset >= file_size:
            mm.close()
            return data

        # Align start_offset
        if start_offset:
            mm.seek(start_offset)
            nl = mm.find(b'\n')
            if nl == -1:
                mm.close()
                return data
            start_offset += nl + 1
            if start_offset >= file_size:
                mm.close()
                return data

        # Align end_offset
        end_offset = min(end_offset, file_size)
        mm.seek(end_offset)
        nl = mm.find(b'\n')
        if nl != -1:
            end_offset += nl + 1
        end_offset = min(end_offset, file_size)

        # Read chunk
        mm.seek(start_offset)
        chunk = mm.read(end_offset - start_offset)
        mm.close()

    # Process lines
    for line in chunk.splitlines():
        if not line:
            continue
        try:
            city, score_str = line.split(b';', 1)
            score = float(score_str)
        except Exception:
            continue

        stats = data[city]
        stats[0] = min(stats[0], score)
        stats[1] = max(stats[1], score)
        stats[2] += score
        stats[3] += 1

    return data

def merge_data(results):
    final = defaultdict(default_city_data)
    for data in results:
        for city, stats in data.items():
            fstats = final[city]
            fstats[0] = min(fstats[0], stats[0])
            fstats[1] = max(fstats[1], stats[1])
            fstats[2] += stats[2]
            fstats[3] += stats[3]
    return final

def get_optimal_cores(file_size):
    # Adjust cores based on file size
    if file_size < 5_000_000:
        return 2
    elif file_size < 25_000_000:
        return 4
    return os.cpu_count()

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    file_size = os.path.getsize(input_file_name)
    num_procs = get_optimal_cores(file_size)
    chunk_size = file_size // num_procs

    chunks = [(i * chunk_size, file_size if i == num_procs - 1 else (i + 1) * chunk_size)
              for i in range(num_procs)]

    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, [(input_file_name, start, end) for start, end in chunks])

    final_data = merge_data(results)

    out_lines = []
    for city in sorted(final_data.keys()):
        mn, mx, total, count = final_data[city]
        avg = round_inf(total / count)
        out_lines.append(f"{city.decode()}={round_inf(mn):.1f}/{avg:.1f}/{round_inf(mx):.1f}\n")

    with open(output_file_name, "w") as f:
        f.writelines(out_lines)

if __name__ == "__main__":
    main()
    