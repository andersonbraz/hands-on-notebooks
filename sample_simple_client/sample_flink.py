import requests
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types


# Function to simulate data extraction from API
def source_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")


# Function to process and map the API response
class ProcessDataMap(MapFunction):
    def map(self, value):
        entry = json.loads(value)
        processed = {
            "lat": entry.get("lat", ""),
            "long": entry.get("long", ""),
            "neighborhoods_sffind_boundaries": entry.get("neighborhoods_sffind_boundaries", ""),
            "address": entry.get("address", ""),
            "service_request_id": entry.get("service_request_id", ""),
            "source": entry.get("source", ""),
            "supervisor_district": entry.get("supervisor_district", ""),
            "service_subtype": entry.get("service_subtype", ""),
        }
        return json.dumps(processed)


# Main Flink job
def etl_job():
    # Step 1: Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Step 2: Define the source (API data as a list of strings)
    data = source_data_from_api("https://data.sfgov.org/resource/33h6-36bw.json")
    
    # Convert the JSON data to a list of strings for processing
    raw_data = [json.dumps(record) for record in data]

    # Step 3: Create a data stream from the list of raw data
    data_stream = env.from_collection(raw_data, type_info=Types.STRING())

    # Step 4: Apply a map transformation to process the data
    processed_stream = data_stream.map(ProcessDataMap(), output_type=Types.STRING())

    # Step 5: Write the processed data to a CSV file (Sink)
    processed_stream.write_as_text("output/san_francisco_incidents.csv").set_parallelism(1)

    # Step 6: Execute the job
    env.execute("San Francisco Incidents ETL Job")


if __name__ == "__main__":
    etl_job()
