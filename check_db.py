#!/usr/bin/env python3
import sqlite3
import os

db_file = 'youtube_trending.db'

if not os.path.exists(db_file):
    print(f"Database file {db_file} does not exist!")
    exit(1)

# Connect to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Check tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in database:")
for table in tables:
    print(f"  - {table[0]}")

# Check record counts
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cursor.fetchone()[0]
    print(f"Table {table[0]} has {count} records")

# Show sample data from trending_videos if it exists
if ('trending_videos',) in tables:
    print("\nSample data from trending_videos:")
    cursor.execute("SELECT video_id, title, channel_title, category_name, view_count FROM trending_videos LIMIT 5")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"Video: {row[1][:30]}... | Channel: {row[2]} | Category: {row[3]} | Views: {row[4]}")
    else:
        print("No data in trending_videos table")

conn.close()