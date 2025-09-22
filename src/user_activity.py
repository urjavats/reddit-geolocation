import os
import json
import gzip
import pandas as pd
import zstandard as zstd

# =========================
# CONFIG
# =========================
AUTHORS_FILE = "../data/author_labels.json.gz"
DUMPS_DIR = "../data/comments/"
OUTPUT_FILE = "../data/user_activity.json.gz"
COMPLETED_USERS_FILE = "../data/completed_users.json"
MAX_COMMENTS_PER_USER = 500

# =========================
# LOAD AUTHORS
# =========================
authors_df = pd.read_json(AUTHORS_FILE, compression='gzip')
authors_set = set(authors_df['author'].tolist())

# =========================
# LOAD EXISTING DATA
# =========================
user_activity = {}
if os.path.exists(OUTPUT_FILE):
    with gzip.open(OUTPUT_FILE, 'rt', encoding='utf-8') as f:
        user_activity = json.load(f)

completed_users = set()
if os.path.exists(COMPLETED_USERS_FILE):
    with open(COMPLETED_USERS_FILE, 'r') as f:
        completed_users = set(json.load(f))

# =========================
# FUNCTION TO READ ZST FILE
# =========================
def read_zst_file(file_path, chunk_size=16384):
    with open(file_path, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)  # allow large windows
        with dctx.stream_reader(fh) as reader:
            buffer = b""
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        yield json.loads(line.decode("utf-8"))
                    except:
                        continue


# =========================
# PROCESS DUMPS
# =========================
dump_files = sorted([f for f in os.listdir(DUMPS_DIR) if f.endswith(".zst")], reverse=True)

for dump_file in dump_files:
    dump_path = os.path.join(DUMPS_DIR, dump_file)
    print(f"\nProcessing dump: {dump_file}")

    count = 0
    for comment in read_zst_file(dump_path):
        author = comment.get('author')
        if author in authors_set and author not in completed_users:
            if author not in user_activity:
                user_activity[author] = []
            user_activity[author].append(comment)
            if len(user_activity[author]) >= MAX_COMMENTS_PER_USER:
                user_activity[author] = user_activity[author][:MAX_COMMENTS_PER_USER]
                completed_users.add(author)

        # simple progress indicator (every 100k comments)
        count += 1
        if count % 100000 == 0:
            print(f"  Processed {count:,} comments...")

    # Append/update progress after each dump
    with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
        json.dump(user_activity, f)
    with open(COMPLETED_USERS_FILE, 'w') as f:
        json.dump(list(completed_users), f)

    # Stop early if all users done
    if len(completed_users) == len(authors_set):
        print("✅ All users have reached the comment limit. You can stop downloading further dumps.")
        break

print(f"\nUser activity extraction complete. Data saved to {OUTPUT_FILE}")
