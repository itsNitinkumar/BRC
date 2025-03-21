import os
from decimal import Decimal, ROUND_CEILING
import concurrent.futures
from array import array
from collections import defaultdict

NUM_PROCESSES = 4
CHUNK_SIZE = 8 * 1024 * 1024

def round_to_infinity(x):
    return float(Decimal(str(x)).quantize(Decimal('0.1'), rounding=ROUND_CEILING))

def process_chunk(chunk_data):
    city_scores = defaultdict(lambda: array('d'))
    
    for line in chunk_data.split(b'\n'):
        line = line.strip()  # Remove any whitespace
        if not line:  # Skip empty lines
            continue
        try:
            city, score = line.split(b';', 1)  # Split on first semicolon only
            city = city.strip().decode('ascii')  # Remove any whitespace and decode
            if not city:  # Skip if city name is empty
                continue
            score = float(score)  # Convert score to float
            city_scores[city].append(score)
        except (ValueError, UnicodeDecodeError):
            continue
            
    return dict(city_scores)

def main(input_file="testcase.txt", output_file="output.txt"):
    try:
        file_size = os.path.getsize(input_file)
        if file_size == 0:
            return

        with open(input_file, 'rb') as f:
            # Read file in chunks
            chunks = []
            remainder = b''
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    if remainder:
                        chunks.append(remainder)
                    break
                
                # Handle chunk boundaries
                if remainder:
                    chunk = remainder + chunk
                    remainder = b''
                
                last_newline = chunk.rfind(b'\n')
                if last_newline != -1:
                    if last_newline < len(chunk) - 1:
                        remainder = chunk[last_newline + 1:]
                        chunk = chunk[:last_newline + 1]
                else:
                    remainder = chunk
                    continue
                
                chunks.append(chunk)

        # Process chunks in parallel
        city_data = defaultdict(lambda: array('d'))
        with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            
            for future in concurrent.futures.as_completed(futures):
                chunk_results = future.result()
                for city, scores in chunk_results.items():
                    city_data[city].extend(scores)

        # Write results
        with open(output_file, 'w', buffering=8*1024*1024) as out:
            for city in sorted(city_data):
                scores = city_data[city]
                if scores:
                    min_score = round_to_infinity(min(scores))
                    max_score = round_to_infinity(max(scores))
                    mean_score = round_to_infinity(sum(scores) / len(scores))
                    out.write(f"{city}={min_score}/{mean_score}/{max_score}\n")

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
