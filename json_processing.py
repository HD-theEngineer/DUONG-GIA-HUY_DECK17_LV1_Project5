import ijson  # Lib to parse large JSON file
import time  # Time lib to measure execution time
import os
import json

##-----------------------------------------------------------------------------------


# Read from json file pool and streaming to batching_data function
def streaming_json(file_pool):
    for file_path in file_pool:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = ijson.items(file, "item")
                yield from data
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")


# receiving file from streaming_json and put data into batches
def batching_data(file_path, batch_size):
    data = []
    batch_no = 0
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            documents = ijson.items(file, "item")
            for doc in documents:
                data.append(doc)
                if len(data) == batch_size:
                    batch_no += 1
                    yield data.copy(), batch_no
                    data.clear()
            if data:
                batch_no += 1
                yield data.copy(), batch_no
                data.clear()
    except Exception as e:
        print(f"Error openning file: {e}")


def product_id_paginate(doc, product_dict):
    id = doc["id"]
    url = doc["url"]
    if id in product_dict and len(product_dict[id]) < 100:
        product_dict[id].append(url)
    elif id in product_dict and len(product_dict[id]) > 100:
        pass
    else:
        product_dict[id] = [url]
    return product_dict


def save_to_json(data, output_dir=None, save_name=None):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, f"batch_{save_name}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved to {file_path}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")


##-----------------------------------------------------------------------------------


def main():
    file_pool = [
        "view_product_detail_L.json",
        "select_product_option_L.json",
        "select_product_option_quality_L.json",
        "add_to_cart_action.json",
        "product_detail_recommendation_visible.json",
        "product_detail_recommendation_noticed.json",
        "product_view_all_recommend_clicked.json",
    ]
    file_path = "D:\\glamira-data\\"

    name_pool = [file_path + file for file in file_pool]
    """
    test_data = [
        {"id": "1234", "url": "url1"},
        {"id": "1235", "url": "url2"},
        {"id": "1236", "url": "url3"},
        {"id": "1234", "url": "url4"},
        {"id": "1237", "url": "url5"},
        {"id": "1236", "url": "url6"},
    ]
    """
    # product_id = {}
    # output_dir = "product_dict\\"
    # final = []

    # start = time.perf_counter()
    # for doc in streaming_json(name_pool):
    #     result = product_id_paginate(doc, product_dict=product_id)

    # for id in result.keys():
    #     result[id].sort()

    # for id, url_list in result.items():
    #     final.append({f"{id}": url_list})

    # save_to_json(final, output_dir)

    for data, batch_no in batching_data("product_dict\\final.json", 5000):
        save_to_json(data, output_dir="product_dict", save_name=f"{batch_no}")

    # end = time.perf_counter()
    # print(f"Total processing time: {end - start} seconds.")


if __name__ == "__main__":
    main()
