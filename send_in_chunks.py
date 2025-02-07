#!/usr/bin/env python3
import argparse
import multiprocessing
import requests
import sys
import time

def get_endpoint(host, backend):
    """
    Return the appropriate ingestion endpoint based on the backend.
    
    For questdb:  HOST/write
    For influxdb: HOST/api/v3/write_lp?db=sensors&precision=auto
    """
    host = host.rstrip('/')
    if backend == "questdb":
        return f"{host}/write"
    elif backend == "influxdb":
        return f"{host}/api/v3/write_lp?db=sensors&precision=auto"
    else:
        raise ValueError(f"Unsupported backend: {backend}")

def send_chunk(chunk_lines, host, backend):
    """
    Sends a POST request with the given chunk of lines to the ingestion endpoint.
    Measures only the HTTP request duration (excluding file reading/chunking) and returns:
        (number_of_rows, elapsed_time)
    """
    url = get_endpoint(host, backend)
    data = "\n".join(chunk_lines) + "\n"
    start_time = time.time()
    try:
        response = requests.post(url, data=data)
        elapsed = time.time() - start_time
        print(f"Sent chunk ({len(chunk_lines)} lines) -> HTTP {response.status_code} in {elapsed:.3f} seconds")
        if not response.ok:
            print(f"Error: {response.text}", file=sys.stderr)
        return (len(chunk_lines), elapsed)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"Exception while sending chunk: {e} (took {elapsed:.3f} seconds)", file=sys.stderr)
        return (len(chunk_lines), elapsed)

def send_chunk_wrapper(args):
    """
    Unpacks the tuple of arguments for use with multiprocessing.Pool.map.
    """
    chunk_lines, host, backend = args
    return send_chunk(chunk_lines, host, backend)

def main():
    parser = argparse.ArgumentParser(
        description="Send file data in parallel chunks (number of lines) to a dynamic endpoint (QuestDB or InfluxDB)."
    )
    parser.add_argument("--host", required=True, help="Host URL (e.g., http://127.0.0.1:9000)")
    parser.add_argument("--file", required=True, help="Path to the input data file")
    parser.add_argument("--chunk-size", type=int, required=True, help="Number of lines per chunk")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers (default: 1)")
    parser.add_argument("--backend", required=True, choices=["questdb", "influxdb"], help="Target backend: questdb or influxdb")
    parser.add_argument("--omit-timestamp", action="store_true", help="If set, remove the timestamp (last token) from each row before sending")
    args = parser.parse_args()

    print(f"\nDividing the file in chunks. This will take a few seconds\n")
    # Prepare chunks as a list of tuples (chunk_lines, host, backend)
    chunks_args = []
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            chunk = []
            for line in f:
                line = line.rstrip("\n")
                if args.omit_timestamp:
                    # Remove the last token (timestamp) along with preceding whitespace.
                    parts = line.rsplit(' ', 1)
                    if len(parts) == 2:
                        line = parts[0]
                chunk.append(line)
                if len(chunk) == args.chunk_size:
                    chunks_args.append((chunk.copy(), args.host, args.backend))
                    chunk = []
            if chunk:
                chunks_args.append((chunk, args.host, args.backend))
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    total_rows = 0
    total_request_time = 0.0

    # Start the wall clock timer for the sending phase.
    start_wall = time.time()
    with multiprocessing.Pool(processes=args.workers) as pool:
        results = pool.map(send_chunk_wrapper, chunks_args)
    end_wall = time.time()
    wall_time = end_wall - start_wall

    for rows, req_time in results:
        total_rows += rows
        total_request_time += req_time

    print(f"\nTotal rows ingested: {total_rows}")
    print(f"Total request time (sum of all HTTP requests): {total_request_time:.3f} seconds")
    print(f"Total wall time for sending requests: {wall_time:.3f} seconds")

if __name__ == '__main__':
    main()

