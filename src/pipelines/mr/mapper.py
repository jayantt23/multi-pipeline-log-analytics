import sys
import re
import os
import json

MASTER_REGEX = re.compile(r'^(\S+) \S+ \S+ \[([^\]]+)\] "([^"]*)" (\d{3}|-) (\d+|-)$')

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def get_batch_id():
    input_file = os.environ.get("mapreduce_map_input_file", "")
    if not input_file:
        input_file = os.environ.get("map_input_file", "")
        
    if not input_file:
        return 1
        
    filename = os.path.basename(input_file)
    if filename.startswith("batch_") and filename.endswith(".log"):
        try:
            return int(filename.split("_")[1].split(".")[0])
        except ValueError:
            pass
            
    file_mapping_str = os.environ.get("BATCH_MAPPING", "{}")
    try:
        mapping = json.loads(file_mapping_str)
        if filename in mapping:
            return mapping[filename]
    except Exception:
        pass
    
    return 1

def main():
    batch_id = get_batch_id()
    query = os.environ.get("MR_QUERY", "all")
    
    for line in sys.stdin:
        # Emit batch stats - we need this for run_stats
        print(f"stats|batch|{batch_id}\t1")
        
        line = line.rstrip('\n')
        if not line:
            print("stats|malformed\t1")
            continue
            
        m = MASTER_REGEX.match(line)
        if not m:
            print("stats|malformed\t1")
            continue
            
        host = m.group(1)
        timestamp_raw = m.group(2)
        request_raw = m.group(3)
        status_raw = m.group(4)
        bytes_raw = m.group(5)
        
        if status_raw == '-':
            print("stats\tmalformed\t1")
            continue
            
        # Parse timestamp: 01/Jul/1995:00:00:01 -0400
        try:
            day = int(timestamp_raw[0:2])
            month_str = timestamp_raw[3:6]
            month_num = MONTH_MAP.get(month_str, 0)
            year = int(timestamp_raw[7:11])
            log_hour = int(timestamp_raw[12:14])
        except Exception:
            # If timestamp parsing fails, just consider malformed for safety,
            # but PARSING_SPEC.md doesn't explicitly mention timestamp format failures
            # However, if it matched the regex, it's mostly fine.
            print("stats\tmalformed\t1")
            continue
            
        log_date = f"{year:04d}-{month_num:02d}-{day:02d}"
        
        # Parse request tokens
        req_tokens = [t for t in request_raw.split(" ") if t != ""]
        resource_path = req_tokens[1] if len(req_tokens) >= 2 else None
        
        status_code = int(status_raw)
        bytes_transferred = int(bytes_raw) if bytes_raw != '-' else 0
        
        # If valid, emit total record
        print("stats|total\t1")
        
        if query in ("1", "all"):
            print(f"q1|{log_date}|{status_code}\t{bytes_transferred}")
            
        if query in ("2", "all") and resource_path is not None:
            # We emit 'q2_metric' for summing
            print(f"q2_metric|{resource_path}\t{bytes_transferred}")
            # We emit 'q2_host' for distinct hosts
            print(f"q2_host|{resource_path}\t{host}")
            
        if query in ("3", "all"):
            print(f"q3_total|{log_date}|{log_hour}\t1")
            if 400 <= status_code <= 599:
                print(f"q3_error|{log_date}|{log_hour}\t1")
                print(f"q3_host|{log_date}|{log_hour}\t{host}")

if __name__ == "__main__":
    main()
