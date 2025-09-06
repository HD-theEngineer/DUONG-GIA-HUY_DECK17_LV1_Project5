from pymongo import MongoClient
import json
from tqdm import tqdm

##-----------------------------------------------------------------------------------

"""
def load_to_mongodb(new_collection, data):
    chunk_data = []
    chunk_size = 1000
    for i in range(0,len(data),chunk_size):
        chunk_data = data[i:i+chunk_size]
        operation = []
        for doc in chunk_data:
            operation.append(
                UpdateOne({'$set': doc}, upsert=True)
            )
        if operation:
            try:
                new_collection.bulk_write(operation, ordered=False)
                print(f"Bulk write {i} documents completed.")
            except Exception as e:
                print(f"Error writing data to mongoDB: {e}")
"""
"""
def insert_to_mongodb(collection,data):
    try:
        collection.insert_many(data,ordered=False)
    except Exception as e:
        print(f"Error during bulk insert: {e}")
"""
##-----------------------------------------------------------------------------------


# Initiate MongoDB connection
def init_mongodb(uri):
    try:
        client = MongoClient(uri)
        print(f"Status: {client.admin.command('ping')}")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


# Streaming documents from MongoDB via stream_dataerator yield
def query_documents(collection, query, projection):
    try:
        data_cursor = collection.find(query, projection)
        for doc in data_cursor:
            yield doc
    except Exception as e:
        print(f"Error during query from MongoDB: {e}")


# Save any yielded data to JSON
def save_to_json(streaming_data, name, total_docs):
    filename = f"{name}.json"
    try:
        with open(filename, "w", encoding="utf-8") as file:
            for doc in tqdm(
                streaming_data, total=total_docs, desc=f"Saving {filename}"
            ):
                json.dump(doc, file, ensure_ascii=False)
                file.write("\n")
        print(f"Saved {total_docs} documents to {filename}.")
    except Exception as e:
        print(f"Error saving {filename}: {e}")


##-----------------------------------------------------------------------------------


def main():
    # Pagination variables
    mongodb_port = 27072
    mongo_uri = f"mongodb://localhost:{mongodb_port}"
    db_name = "project-5"
    collection_name = "summary"
    field_to_filter = {
        "collection": [
            "product_view_all_recommend_clicked",
            "add_to_cart_action",
            "product_detail_recommendation_visible",
            "product_detail_recommendation_noticed",
            "view_product_detail",
            "select_product_option",
            "select_product_option_quality",
        ]
    }
    # Define the mapping between collection values and fields to get
    query_fields_map = {
        "product_view_all_recommend_clicked": ["viewing_product_id", "referrer_url"],
        "add_to_cart_action": ["product_id", "current_url"],
        "product_detail_recommendation_visible": ["viewing_product_id", "current_url"],
        "product_detail_recommendation_noticed": ["viewing_product_id", "current_url"],
        "view_product_detail": ["product_id", "current_url"],
        "select_product_option": ["product_id", "current_url"],
        "select_product_option_quality": ["product_id", "current_url"],
        # Add more mappings as needed
    }

    # Connect to mongoDB
    client = init_mongodb(mongo_uri)
    if not client:
        return None
    # Choose database and collection
    db = client[db_name]
    collection = db[collection_name]

    for key, values in field_to_filter.items():
        for value in values:
            fields_to_get = query_fields_map.get(value, [])
            if not fields_to_get:
                print(
                    f"No field mapping found for collection value: {value}, skipping."
                )
                continue
            projection = {field: 1 for field in fields_to_get}
            projection["_id"] = 0
            query = {key: value}
            total_docs = collection.count_documents(query)
            print(f"Total {value} documents to filter: {total_docs}")
            stream_data = query_documents(collection, query, projection)
            save_to_json(stream_data, value, total_docs)

    client.close()
    print("Processing complete. MongoDB connection closed.")


if __name__ == "__main__":
    main()
