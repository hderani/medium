# Databricks notebook source
# DBTITLE 1,Write initial JSON files
import os
import shutil
import json
from datetime import datetime, timedelta

base_dir = "/dbfs/tmp/airline_stream_demo"
dirs = ["flight_delay", "profile_updates", "preference_updates"]

# Clear and recreate directories
for d in dirs:
    path = os.path.join(base_dir, d)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

# Prepare sample data batches
flight_delay_data = [
    {
        "user_id": "U1",
        "event_type": "delay",
        "timestamp": datetime.now().isoformat(),
        "flight_id": "F100",
        "delay_duration": "15m",
    },
    {
        "user_id": "U2",
        "event_type": "delay",
        "timestamp": (datetime.now() + timedelta(seconds=5)).isoformat(),
        "flight_id": "F200",
        "delay_duration": "30m",
    },
]
profile_data = [
    {
        "user_id": "U1",
        "name": "Alice Smith",
        "email": "alice@example.com",
        "updated_at": datetime.now().isoformat(),
    },
    {
        "user_id": "U2",
        "name": "Bob Jones",
        "email": "bob@example.com",
        "updated_at": (datetime.now() + timedelta(seconds=3)).isoformat(),
    },
]
preference_data = [
    {
        "user_id": "U1",
        "preferred_category": "Business",
        "updated_at": datetime.now().isoformat(),
    },
    {
        "user_id": "U3",
        "preferred_category": "Economy",
        "updated_at": (datetime.now() + timedelta(seconds=7)).isoformat(),
    },
]


# Write JSON files batch1.json (simulate first micro-batch)
def write_json(data, folder, filename):
    with open(os.path.join(base_dir, folder, filename), "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")


write_json(flight_delay_data, "flight_delay", "batch1.json")
write_json(profile_data, "profile_updates", "batch1.json")
write_json(preference_data, "preference_updates", "batch1.json")

# COMMAND ----------

# DBTITLE 1,Simulated Streaming Data
import os
import json
import time
from datetime import datetime, timedelta
import random

base_dir = "/dbfs/tmp/airline_stream_demo"

flight_delay_dir = os.path.join(base_dir, "flight_delay")
profile_updates_dir = os.path.join(base_dir, "profile_updates")
preference_updates_dir = os.path.join(base_dir, "preference_updates")

user_ids = ["U1", "U2", "U3", "U4", "U5"]
flight_ids = ["F100", "F200", "F300", "F400"]
delay_durations = ["5m", "10m", "15m", "30m"]

preferred_categories = ["Business", "Economy", "First Class"]
names_emails = {
    "U1": ("Alice Smith", "alice@example.com"),
    "U2": ("Bob Jones", "bob@example.com"),
    "U3": ("Charlie Day", "charlie@example.com"),
    "U4": ("Diana Prince", "diana@example.com"),
    "U5": ("Ethan Hunt", "ethan@example.com"),
}


def write_json(data, folder, batch_id):
    filepath = os.path.join(folder, f"batch_{batch_id}.json")
    with open(filepath, "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")


batch_id = 2

try:
    while True:
        # Create flight delay events
        flight_delay_data = []
        for _ in range(random.randint(1, 3)):
            user = random.choice(user_ids)
            flight = random.choice(flight_ids)
            delay = random.choice(delay_durations)
            event = {
                "user_id": user,
                "event_type": "delay",
                "timestamp": datetime.now().isoformat(),
                "flight_id": flight,
                "delay_duration": delay,
            }
            flight_delay_data.append(event)

        write_json(flight_delay_data, flight_delay_dir, batch_id)

        # Create profile updates (randomly update some users)
        profile_data = []
        for _ in range(random.randint(0, 2)):
            user = random.choice(user_ids)
            name, email = names_emails[user]
            profile_update = {
                "user_id": user,
                "name": name,
                "email": email,
                "updated_at": datetime.now().isoformat(),
            }
            profile_data.append(profile_update)

        if profile_data:
            write_json(profile_data, profile_updates_dir, batch_id)

        # Create preference updates
        preference_data = []
        for _ in range(random.randint(0, 2)):
            user = random.choice(user_ids)
            pref = random.choice(preferred_categories)
            preference_update = {
                "user_id": user,
                "preferred_category": pref,
                "updated_at": datetime.now().isoformat(),
            }
            preference_data.append(preference_update)

        if preference_data:
            write_json(preference_data, preference_updates_dir, batch_id)

        print(
            f"Wrote batch {batch_id} with {len(flight_delay_data)} delay events, "
            f"{len(profile_data)} profile updates, {len(preference_data)} preference updates"
        )

        batch_id += 1
        time.sleep(10)  # wait 10 seconds before writing next batch

except KeyboardInterrupt:
    print("Stopped simulation.")