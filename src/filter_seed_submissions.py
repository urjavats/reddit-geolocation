import os
import json
import zstandard as zstd
import ndjson
import io
import re

data_folder = "data/submissions"

# Broader regex patterns to identify seed submissions
keywords = [
    "where are you from",
    "where do you live",
    "where are you living",
    "share your location",
    "post your city",
    "post your country",
    "your hometown",
    "what's your city",
    "what's your state",
    "what's your country",
    "where do you call home",
    "show your hometown",
    "reddit: where are you from",
    "where is everyone from",
    "your location?",
    "where in the world are you",
    "tell us your location",
    "city/state/country",
    "who owns your location",
    "current city",
    "where do you currently live"
]


def read_zst_file(file_path):
    submissions = []
    with open(file_path, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(fh) as reader:
            text_stream = ndjson.reader(io.TextIOWrapper(reader, encoding='utf-8'))
            for obj in text_stream:
                submissions.append(obj)
    return submissions

# Filter seed submissions
seed_submissions = []

for file_name in sorted(os.listdir(data_folder)):
    if file_name.endswith(".zst"):
        file_path = os.path.join(data_folder, file_name)
        print(f"\nReading {file_name}...")
        subs = read_zst_file(file_path)

        for sub in subs:
            title = sub.get("title", "").lower()  # lowercase for case-insensitive matching

            # Check if any keyword is in the title
            if any(kw in title for kw in keywords):
                filtered_sub = {
                    "id": sub["id"],
                    "author": sub.get("author", ""),
                    "title": sub.get("title", ""),
                    "subreddit": sub.get("subreddit", ""),
                    "created_utc": sub.get("created_utc", 0)
                }

                seed_submissions.append(filtered_sub)
                print("Relevant submission found:")
                print(json.dumps(filtered_sub, indent=2, ensure_ascii=False))

print(f"\nTotal seed submissions found: {len(seed_submissions)}")

# Save to a new file to keep old results intact
with open("data/seed_submissions_2007.json", "w", encoding="utf-8") as f:
    json.dump(seed_submissions, f, indent=2)

print("\nSeed submissions saved to data/seed_submissions_2007.json")
