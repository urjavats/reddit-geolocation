import gzip
import json

def peek_json_gz(filepath, n=5):
    """Print first n records from a .json.gz file to inspect structure."""
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            try:
                record = json.loads(line)
                print(json.dumps(record, indent=2))
            except json.JSONDecodeError as e:
                print(f"Error parsing line {i}: {e}")
                print(line)

if __name__ == "__main__":
    filepath = "../data/author_labels.json.gz"  # update path if needed
    peek_json_gz(filepath, n=5)
