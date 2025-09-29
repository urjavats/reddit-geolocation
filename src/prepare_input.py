import pandas as pd
import json
import gzip
import re
import os 
import random

# =========================
# FILE PATHS
# =========================
SUBMISSIONS_FILE = "../data/seed_submissions.csv"
COMMENTS_FILE = "../data/seed_comments.csv"
USER_ACTIVITY_FILE="../data/user_activity.json.gz"
LABELS_FILE="../data/author_labels.json.gz"
OUTPUT_FILE_JSON = "../data/final_input.json"

# =========================
# LOAD AUTHOR LABELS (master list of users)
# =========================
with gzip.open(LABELS_FILE, 'rt', encoding='utf-8') as f:
    author_labels_data = json.load(f)

# Create dict: author -> labels
author_labels = {}
authors_set = set()
for entry in author_labels_data:
    author = entry['author']
    authors_set.add(author)
    author_labels[author] = {
        'locality': entry.get('locality'),
        'administrative_area_level_1': entry.get('administrative_area_level_1'),
        'country': entry.get('country')
         }

# =========================
# HELPER FUNCTIONS
# =========================
def clean_text(text):
    """Clean and normalize text for NLP input."""
    if not isinstance(text, str):
        return ""
    # Lowercase
    text = text.lower()
    # Remove URLs
    text = re.sub(r"http\S+", "", text)
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

# =========================
# LOAD SUBMISSIONS
# =========================
sub_df = pd.read_csv(SUBMISSIONS_FILE)

# Selected fields and reasons:
# 'author' -> To group submissions per user
# 'title' -> Main textual content
# 'selftext' -> Optional extra textual content
# 'subreddit' -> Can indicate location / interest group
# 'created_utc' -> Timing signal for activity patterns
# 'score','num_comments' -> Engagement metrics
sub_fields = ['author', 'title', 'selftext', 'subreddit', 'created_utc', 'score','num_comments']
sub_df = sub_df[sub_fields]

# Merge title + selftext as main text
sub_df['text'] = (sub_df['title'].fillna('') + " " + sub_df['selftext'].fillna('')).apply(clean_text)

# Create dict: author -> list of submissions
user_submissions = {}
for _, row in sub_df.iterrows():
    author = row['author']
    entry = {
        'text': row['text'],
        'metadata': {
            'subreddit': row['subreddit'],
            'created_utc': row['created_utc'],
            'score': row['score'],
            'num_comments': row['num_comments']
        }
    }
    user_submissions.setdefault(author, []).append(entry)

# =========================
# LOAD COMMENTS
# =========================
com_df = pd.read_csv(COMMENTS_FILE)

# Selected fields and reasons:
# 'author' -> needed to group comments per user
# 'body' -> main text content
# 'created_utc' -> timestamp for activity patterns
# 'subreddit' -> context, may hint location
# 'author_flair_text' -> optional flair info (may contain location/group info)
# 'score' -> engagement metrics
com_fields = ['author', 'body', 'created_utc', 'subreddit', 'author_flair_text', 'score','parent_id']
com_df = com_df[com_fields]

# Clean comment text
com_df['text'] = com_df['body'].apply(clean_text)

# Create dict: author -> list of comments
user_comments = {}
for _, row in com_df.iterrows():
    author = row['author']
    entry = {
        'text': row['text'],
        'metadata': {
            'subreddit': row['subreddit'],
            'created_utc': row['created_utc'],
            'author_flair_text': row['author_flair_text'],
            'score': row['score'],
            'parent_id': row['parent_id']
        }
    }
    user_comments.setdefault(author, []).append(entry)
# =========================
# LOAD USER ACTIVITY
# =========================
user_activity = {}
if os.path.exists(USER_ACTIVITY_FILE):
    with gzip.open(USER_ACTIVITY_FILE, 'rt', encoding='utf-8') as f:
        user_activity = json.load(f)
# =========================
# MERGE SUBMISSIONS AND COMMENTS PER USER
# =========================
user_input_data = {}

for user in authors_set:
    user_input_data[user] = {
        'submissions': user_submissions.get(user, []),
        'comments': user_comments.get(user, []),
        'user_activity': user_activity.get(user, []),
        'labels': author_labels[user]
    }

# =========================
# SAVE OUTPUT
# =========================
# JSON (keeps structure for flexible downstream use)
with open(OUTPUT_FILE_JSON, 'w', encoding='utf-8') as f:
    json.dump(user_input_data, f, indent=2)

print(f"Final input JSON saved at {OUTPUT_FILE_JSON}")

#delete
if len(user_input_data) > 0:
    preview_users = random.sample(list(user_input_data.keys()), min(10, len(user_input_data)))
    preview_data = {u: user_input_data[u] for u in preview_users}
    with open(PREVIEW_FILE_JSON, 'w', encoding='utf-8') as f:
        json.dump(preview_data, f, indent=2)
    print(f"Preview (10 users) saved at {PREVIEW_FILE_JSON}")


