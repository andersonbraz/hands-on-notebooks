import requests
import csv

# Define the source (endpoint URL)
URL = "https://data.sfgov.org/resource/33h6-36bw.json"

# Function to simulate the data source in an Apache Flink job
def source_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

# Function to process the data (transformation step in Flink)
def process_data(data):
    processed_data = []
    for entry in data:
        processed_data.append({
            "lat": entry.get("lat", ""),
            "long": entry.get("long", ""),
            "neighborhoods_sffind_boundaries": entry.get("neighborhoods_sffind_boundaries", ""),
            "address": entry.get("address", ""),
            "service_request_id": entry.get("service_request_id", ""),
            "source": entry.get("source", ""),
            "supervisor_district": entry.get("supervisor_district", ""),
            "service_subtype": entry.get("service_subtype", ""),
        })
    return processed_data

# Function to write data to CSV (sink in Flink)
def write_data_to_csv(data, file_name):
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["lat", "long", "neighborhoods_sffind_boundaries", "address", "service_request_id", "source", "supervisor_district", "service_subtype"])
        writer.writeheader()
        writer.writerows(data)

# Main function to orchestrate the ETL job
def etl_job():
    try:
        # Step 1: Source (Extract)
        data = source_data_from_api(URL)
        
        # Step 2: Transformation
        processed_data = process_data(data)
        
        # Step 3: Sink (Load)
        write_data_to_csv(processed_data, 'san_francisco_incidents.csv')
        print("ETL job completed successfully and data written to san_francisco_incidents.csv")
    except Exception as e:
        print(f"ETL job failed: {e}")

# Run the ETL job
if __name__ == "__main__":
    etl_job()


