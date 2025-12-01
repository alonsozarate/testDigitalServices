# Data Generator

import pandas as pd
import json
import os
import random
import yaml
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def load_config():
    with open("config/etl_config.yaml","r") as f:
        return yaml.safe_load(f)

def generate_mock_data():
    print("Cargando la configuración")
    config = load_config()
    paths = config["data_paths"]
    
    print("Generating test data")
    os.makedirs(paths["raw_events"], exist_ok = True)
    os.makedirs(paths["raw_transactions"], exist_ok = True)
    os.makedirs(paths["raw_users"], exist_ok = True)

    # Users - Dimensional
    users = []
    user_ids = [f"U{i:03d}" for i in range(1,31)]
    for uid in user_ids:
        users.append({
            "user_id": uid,
            "signup_date": fake.date_this_year().isoformat(),
            "device_type": random.choice(["iOS","Android","Web"]),
            "country": random.choice(["Peru","Mexico","Colombia"])
        })
    
    pd.DataFrame(users).to_csv(f"{paths['raw_users']}users.csv", index=False)
    
    # Events - streaming - Json
    events = []
    session_ids = [f"S{i:03d}" for i in range(1,51)]
    event_types = ["login","ver_producto","add_cart","checkout","logout"]
    
    for _ in range(200):
        user = random.choice(user_ids)
        session = random.choice(session_ids)
        events.append({
            "event_id":fake.uuid4(),
            "user_id": user,
            "session_id": session,
            "event_type": random.choice(event_types),
            "event_timestamp": fake.iso8601(),
            "event_details": {
                "url": fake.url(),
                "metadata": "test"
            }
        })
        
    with open(f"{paths['raw_events']}events.json", "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
            
    # Transactions - Batch - CSV
    transactions = []
    for _ in range(20):
        evt = random.choice(events)
        transactions.append({
            "transaction_id": fake.uuid4(),
            "session_id": evt["session_id"],
            "user_id": evt["user_id"],
            "amount": round(random.uniform(10.0,500.0),2),
            "currency": "SOL",
            "transaction_timestamp": evt["event_timestamp"]
        })
        
    pd.DataFrame(transactions).to_csv(
        f"{paths['raw_transactions']}transactions.csv", index = False
    )
    
    print("Data generada para localizado en la carpeta data/bronze")
    
if __name__ == "__main__":
    generate_mock_data()
        
    
    