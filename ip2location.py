from pymongo import MongoClient
import IP2Location
import os
import json


def init_mongoDB(uri):
    try:
        # Initiate connection to MongoDB
        client = MongoClient(uri)

        # Tesst connection
        client.admin.command("ping")  # Ping to test the connection
        print(f"Successfully connected to MongoDB at {uri}.")
        return client

    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def load_ip2location(path):
    # Load IP2Location database
    try:
        ip_db = IP2Location.IP2Location(str(path))
        print("Load ip2location data sucessfully.")
        return ip_db
    except FileNotFoundError:
        print(f"Error: IP2Location database not found at {path}")
        return


def batch_query_mongodb(collection, batch_size):
    last_ip = None
    while True:
        # Build the aggregation pipeline
        pipeline = []
        # If this is not the first batch, add a $match stage to start from the last processed IP
        if last_ip:
            pipeline.append({"$match": {"ip": {"$gt": last_ip}}})
        # Group by IP to get unique values
        pipeline.append({"$group": {"_id": "$ip"}})
        # Sort the unique IPs to ensure a consistent order
        pipeline.append({"$sort": {"_id": 1}})
        # Limit the number of documents in this batch
        pipeline.append({"$limit": batch_size})
        print(f"Running this pipeline:... \n{pipeline}")

        # Execute the pipeline
        ip_batch_cursor = collection.aggregate(pipeline)
        ip_list = list(ip_batch_cursor)

        # If the batch is empty, we've processed all documents
        if not ip_list:
            print("\nAll unique IPs have been processed.")
            break

        # Update last IP for nexxt batch
        last_ip = ip_list[-1]["_id"]
        print(f"Last IP of this batch: {last_ip}")
        yield ip_list


def convert_ip2location(ip_list, ip_db):
    enriched_data = []  # initiate data variable
    # Process the batch
    for doc in ip_list:
        ip = doc["_id"]
        # Look up location using IP2Location
        try:
            rec = ip_db.get_all(ip)

            converted_data = {"ip": ip, "country": rec.country_long, "city": rec.city}
            enriched_data.append(converted_data)
        except Exception as e:
            print(f"Error looking up IP {ip}: {e}")
    return enriched_data


def save_to_json(data, data_num, output_dir="data_batches"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, f"ip_location_batch_{data_num}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved batch {data_num} to {file_path}")
    except Exception as e:
        print(f"Error saving batch {data_num} to JSON: {e}")


def main():
    # Pagination variables
    MONGODB_PORT = 27072
    MONGO_URI = str(f"mongodb://localhost:{MONGODB_PORT}")
    db_name = "project-5"
    collection_name = "summary"
    IP2LOCATION_DB = "IP2LOCATION-LITE-DB3.IPV6.BIN"

    client = init_mongoDB(MONGO_URI)
    # Choose database and collection
    db = client[db_name]
    coll = db[collection_name]

    # Load ip2location
    ip_db = load_ip2location(IP2LOCATION_DB)

    # Process IPs by batch
    processed_count = 0
    batch_num = 0

    for batch in batch_query_mongodb(coll, batch_size=1000):
        batch_num += 1
        print(f"Processing batch #{batch_num} with {len(batch)} unique IPs...")

        # Proceed to convert IP in list to location
        enriched_ip_data = convert_ip2location(batch, ip_db)
        processed_count += len(batch)
        print(f"Processed {processed_count} unique IPs so far.")

        # Save data into json
        save_to_json(enriched_ip_data, batch_num)

    # lose the MongoDB connection
    client.close()
    print("Processing complete. MongoDB connection closed.")


if __name__ == "__main__":
    main()
