import sys

def output_result(current_key, values):
    if not current_key:
        return
        
    parts = current_key.split('|', 1)
    prefix = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    
    if prefix == "stats":
        if rest == "total" or rest == "malformed":
            total = sum(int(v) for v in values)
            print(f"{prefix}|{rest}\t{total}")
        elif rest.startswith("batch|"):
            # values are just '1' for each record, we don't need to count them, we just need the distinct batch_id
            print(f"{prefix}|{rest}\t1")
    elif prefix == "q1":
        request_count = len(values)
        total_bytes = sum(int(v) for v in values)
        print(f"q1|{rest}\t{request_count}\t{total_bytes}")
    elif prefix == "q2_metric":
        request_count = len(values)
        total_bytes = sum(int(v) for v in values)
        print(f"q2_metric|{rest}\t{request_count}\t{total_bytes}")
    elif prefix == "q2_host":
        distinct_hosts = len(set(values))
        print(f"q2_host|{rest}\t{distinct_hosts}")
    elif prefix == "q3_total":
        total_request_count = len(values)
        print(f"q3_total|{rest}\t{total_request_count}")
    elif prefix == "q3_error":
        error_request_count = len(values)
        print(f"q3_error|{rest}\t{error_request_count}")
    elif prefix == "q3_host":
        distinct_error_hosts = len(set(values))
        print(f"q3_host|{rest}\t{distinct_error_hosts}")

def main():
    current_key = None
    current_values = []
    
    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue
            
        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue
            
        key, value = parts[0], parts[1]
        
        if key == current_key:
            current_values.append(value)
        else:
            if current_key is not None:
                output_result(current_key, current_values)
            current_key = key
            current_values = [value]
            
    if current_key is not None:
        output_result(current_key, current_values)

if __name__ == "__main__":
    main()
