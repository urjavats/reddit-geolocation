import gzip
import json

INPUT_FILE = "../data/user_activity.json.gz"
OUTPUT_FILE = "../data/user_activity_preview.json"
NUM_USERS_TO_PREVIEW = 5  # Number of users to include in preview
NUM_COMMENTS_PER_USER = 3  # Number of comments per user to include

preview_data = {}

# Read the gzipped user activity file
with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f:
    data = json.load(f)

# Take first few users and their first few comments
for i, (user, comments) in enumerate(data.items()):
    if i >= NUM_USERS_TO_PREVIEW:
        break
    preview_data[user] = comments[:NUM_COMMENTS_PER_USER]

# Save to a readable JSON file
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(preview_data, f, indent=2)

print(f"Preview saved to {OUTPUT_FILE}")
