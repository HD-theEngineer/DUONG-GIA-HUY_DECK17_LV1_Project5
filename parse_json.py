import json
import time  # Time lib to measure execution time

##-----------------------------------------------------------------------------------
"""
Function to receiving package from streaming_json and extract only "product_id" and
"current_url" keys (remove the "_id" keys), change them into "id" and "url" and
then write back to json
"""


def extract_json(name_pool):
    for file_name in name_pool:
        output_data = []
        output_file = file_name.replace(".json", "_L.json")
        try:
            with open(file_name, "r", encoding="utf-8") as file:
                for line in file:
                    # Remove _id and map keys
                    item = json.loads(line)
                    new_doc = {}
                    new_doc["id"] = item.get("product_id")
                    new_doc["url"] = item.get("current_url")
                    output_data.append(new_doc)

            # Write to a new file
            with open(output_file, "w", encoding="utf-8") as out_file:
                json.dump(output_data, out_file, ensure_ascii=False, indent=4)
            print(f"Extracted data saved to {output_file}")
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")


##-----------------------------------------------------------------------------------


def main():
    file_pool = [
        "view_product_detail.json",
        "select_product_option.json",
        "select_product_option_quality.json",
    ]
    file_path = "D:\\glamira-data\\"

    name_pool = [file_path + file for file in file_pool]
    start_time = time.perf_counter()
    extract_json(name_pool)
    end_time = time.perf_counter()
    print(f"Total processing time: {end_time - start_time} seconds.")


if __name__ == "__main__":
    main()
