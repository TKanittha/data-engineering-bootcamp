import configparser
import csv

import requests


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
DATA_FOLDER = "data"

### Events
data_name = "events"
date = "2021-02-10"
response = requests.get(f"{API_URL}/{data_name}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/events.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Users
data_name = "users"
date = "2020-10-23"
# ลองดึงข้อมูลจาก API เส้น users และเขียนลงไฟล์ CSV
# YOUR CODE HERE
response = requests.get(f"{API_URL}/{data_name}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/users.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Orders
data_name = "orders"
date = "2021-02-10"
# ลองดึงข้อมูลจาก API เส้น orders และเขียนลงไฟล์ CSV
# YOUR CODE HERE
# esponse = requests.get(f"{API_URL}/{data}/?created_at={date}")

response = requests.get(f"{API_URL}/{data_name}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/{data_name}.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())


table_list = [
        {
            data_name: "events"
            , date: "2021-02-10"
        }
        , 
        {
            data_name: "users"
            , date: "2020-10-23"
        }
        ,
        {
            data_name: "orders"
            , date: "2021-02-10"
        }
    ]

    
for each in table_list:
        print(each[data_name])
        print(each[date])