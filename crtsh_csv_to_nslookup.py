#!/usr/bin/env python3
"""
a script to read a CSV file (e.g. from crt.sh) and run nslookup or dig on each entry in a specified column and save results to a text file. 

Usage:
    `python3 crtsh_csv_nslookup.py input.csv ColumnName output.txt`

Options:
    - you can set environment variables or edit variables below:
        QUERY_CMD = "nslookup" or "dig"
"""

import csv
import sys
import subprocess
import concurrent.futures
from pathlib import Path
from datetime import datetime

# ---------- CONFIG ----------
QUERY_CMD = "nslookup"   # or "dig"
NSLOOKUP_TYPE = "-type=TXT"       # e.g., "-type=TXT" for nslookup; leave "" for default
DIG_TYPE = "TXT"         # if using dig, we will use: dig +short TXT host
TIMEOUT = 6              # seconds per query
MAX_WORKERS = 12         # parallel threads
# ----------------------------

def make_nslookup_cmd(host):
    host = host.strip()
    if not host:
        return None
    if QUERY_CMD == "nslookup":
        if NSLOOKUP_TYPE:
            return ["nslookup", NSLOOKUP_TYPE, host]
        return ["nslookup", host]
    elif QUERY_CMD == "dig":
        # use +short for compact output
        return ["dig", "+short", f"TXT", host]
    else:
        raise ValueError("QUERY_CMD must be 'nslookup' or 'dig'")

def run_query(host):
    cmd = make_nslookup_cmd(host)
    if cmd is None:
        return host, "EMPTY_HOSTNAME", ""
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        rc = completed.returncode
        out = f"RETURN_CODE={rc}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    except subprocess.TimeoutExpired:
        out = f"ERROR: timeout after {TIMEOUT}s"
    except Exception as e:
        out = f"ERROR: {e}"
    return host, "OK", out

def write_header(outfh, csv_file, column, total):
    outfh.write(f"# csv_nslookup.py results\n")
    outfh.write(f"# source CSV: {csv_file}\n")
    outfh.write(f"# column: {column}\n")
    outfh.write(f"# date: {datetime.utcnow().isoformat()}Z\n")
    outfh.write(f"# total hosts: {total}\n\n")

def main(csv_path, column_name, out_path):
    csv_path = Path(csv_path)
    out_path = Path(out_path)

    hosts = []
    with csv_path.open(newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        if column_name not in reader.fieldnames:
            print(f"ERROR: column '{column_name}' not found. Available columns: {reader.fieldnames}")
            sys.exit(2)
        for row in reader:
            value = row.get(column_name, "").strip()
            if value:
                hosts.append(value)

    if not hosts:
        print("No hosts found in that column.")
        return

    with out_path.open("w", encoding='utf-8') as outfh:
        write_header(outfh, csv_path.name, column_name, len(hosts))

        # run in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(run_query, h): h for h in hosts}
            for fut in concurrent.futures.as_completed(futures):
                host = futures[fut]
                try:
                    hostname, status, output = fut.result()
                except Exception as e:
                    hostname = host
                    status = "EXC"
                    output = f"ERROR: {e}"
                outfh.write(f"--- {hostname} ---\n")
                outfh.write(output)
                outfh.write("\n\n")

    print(f"Done. Results saved to {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 csv_nslookup.py input.csv ColumnName output.txt")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])

