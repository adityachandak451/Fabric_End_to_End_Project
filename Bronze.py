import requests
import json
from pyspark.sql import SparkSession

url = "https://disease.sh/v3/covid-19/all"

response = requests.get(url)

if response.status_code ==200:
    data = response.json()



# Save JSON to a file in Lakehouse (Files section)
    file_path = '/lakehouse/default/Files/test.json'  # Fabric DBFS path

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print("data loaded")

else:
    print("not load", response.status_code)
