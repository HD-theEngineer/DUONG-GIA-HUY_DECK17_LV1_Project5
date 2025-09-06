import json  # LIb to work with JSON file
import requests  # Lib to send HTTP request and receive HTML source code
import time  # Time lib to measure execution time
import random  # Randomizer lib to random the sleep time and randomly select user-agent
import logging
import re
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    TimeoutError,
)  # Lib to create multi-thread runs
from threading import Event
from json_processing import streaming_json, save_to_json

MAX_WORKERS = 6
REQUEST_TIMEOUT = 12  # Tăng timeout lên một chút để xử lý các trang load chậm
MAX_RETRIES = 3  # Số lần thử lại tối đa cho một URL
RETRY_BACKOFF_FACTOR = 2  # Hệ số nhân cho thời gian chờ giữa các lần retry
keys_map = [
    "product_id",
    "name",
    "sku",
    "attribute_set_id",
    "attribute_set",
    "type_id",
    "price",
    "min_price",
    "max_price",
    "min_price_format",
    "max_price_format",
    "gold_weight",
    "none_metal_weight",
    "fixed_silver_weight",
    "material_design",
    "qty",
    "collection",
    "collection_id",
    "product_type",
    "product_type_value",
    "category",
    "category_name",
    "store_code",
    "show_popup_quantity_eternity",
    "visible_contents",
    "gender",
]
logging.basicConfig(
    filename="scraping_glamira.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
stop_all_workers = Event()
##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


def product_scraping(id_url, headers_template=None, user_agents=None):
    result = None
    faulty_package = []

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {}
            for id, url_list in id_url.items():
                if stop_all_workers.is_set():
                    break  # Break the loop if stop signal is already set

                for url in url_list:
                    try:
                        headers = {
                            **headers_template,
                            "User-Agent": random.choice(user_agents),
                        }

                        future = executor.submit(
                            request_data, session, url, headers, stop_all_workers
                        )
                        future_to_id[future] = future
                    except Exception as e:
                        print(f"Error during submission: {e}")
                        logging.error(f"Error during submission: {e}")
                        return result, faulty_package

                while future_to_id:
                    done_futures = as_completed(future_to_id, timeout=6)
                    future = next(done_futures)
                    original_data = future_to_id.pop(future)
                    product_data = future.result()
                    try:
                        if "success" in product_data:
                            result = product_data["success"]
                            logging.info(f"Successfully added to result: {url}")
                            stop_all_workers.set()
                            for f in future_to_id:
                                f.cancel()
                            future_to_id.clear()
                            break
                        elif "retry" in product_data:
                            retry_count = product_data.get("retry_count")
                            if retry_count < MAX_RETRIES:
                                sleep_time = random.uniform(0.5, 1.5) * (
                                    RETRY_BACKOFF_FACTOR**retry_count
                                )
                                time.sleep(sleep_time)

                                # retry submitting task
                                data_to_retry = product_data.get("retry")
                                prev_user_agent = product_data["ua"]
                                available_agents = [
                                    ua for ua in user_agents if ua != prev_user_agent
                                ]
                                if not available_agents:
                                    available_agents = (
                                        user_agents  # fallback if all are used
                                    )

                                new_headers = {
                                    **headers_template,
                                    "User-Agent": random.choice(available_agents),
                                }
                                new_future = executor.submit(
                                    request_data,
                                    session,
                                    data_to_retry,
                                    new_headers,
                                    stop_all_workers,
                                    retry_count,
                                )
                                future_to_id[new_future] = data_to_retry
                            else:
                                faulty_package.append(product_data["failed"])
                                logging.warning(
                                    f"Max retries reached for {data_to_retry}"
                                )
                            pass
                        else:
                            faulty_package.append(product_data)
                            logging.warning(
                                f"Faulty result for id {id}: {product_data}"
                            )
                    except TimeoutError:
                        # Timeout reached, check if stop_all_workers is set
                        print("Waiting for futures to complete...")
                        if stop_all_workers.is_set():
                            break
                    except Exception as e:
                        original_data = product_data
                        logging.error(f"Error processing: {e}")
                        faulty_package.append(
                            {
                                f"{e}": original_data.items(),
                            }
                        )
    return result, faulty_package


def request_data(session, url, headers, stop_event, retry_count=0):
    if stop_event.is_set():
        return {"cancelled": url}  # Immediately return if the stop event is set

    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code in [200, 201]:
            product_data = [{"url": url}]
            logging.info(f"Successed: {url}")
            react_data = extract_react_data(response.text)
            for key, value in react_data.items():
                if key in keys_map:
                    product_data.append({key: value})
            return {
                "success": product_data,
            }

        elif response.status_code in [403, 429, 500, 502, 503, 504]:
            logging.warning(f"Retryable error {response.status_code} for {url}")
            return {
                "retry": url,
                "ua": headers["User-Agent"],
                "retry_count": retry_count + 1,
            }
        else:
            logging.error(f"Non-retryable error {response.status_code} for {url}")
            return {f"{response.status_code}": url}
    except requests.exceptions.RequestException as e:
        logging.error(f"Exception occurred: {e}.")
        return {
            "retry": url,
            "ua": headers["User-Agent"],
            "retry_count": retry_count + 1,
        }


def extract_react_data(html_text):
    match = re.search(r"var\s+react_data\s*=\s*(\{.*?\});", html_text, re.DOTALL)
    if match:
        json_text = match.group(1)
        # Sometimes JS uses single quotes or has trailing commas; fix if needed
        try:
            data = json.loads(json_text)
        except Exception:
            # Try to fix common JS-to-JSON issues
            json_text = json_text.replace("'", '"')
            json_text = re.sub(r",\s*}", "}", json_text)
            json_text = re.sub(r",\s*]", "]", json_text)
            data = json.loads(json_text)
        return data
    return None


##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


def main():
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    ]

    headers_template = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": "https://glamira.vn/",
    }

    file_path = ["product_dict\\batch_4.json"]
    faulty_output_dir = "faulty\\"
    result_output_dir = "result\\"

    # with open("product_dict\\test.json", "r") as f:
    #     test_id_url = json.load(f)

    global stop_all_workers
    stop_all_workers.clear()

    final_result = []
    final_faulty = []

    for id_url in streaming_json(file_path):
        start_time = time.perf_counter()

        result, faulty_package = product_scraping(id_url, headers_template, user_agents)
        print(result)
        stop_all_workers.clear()

        final_result.append(result)
        final_faulty.append(faulty_package)

        end_time = time.perf_counter()
        print(f"Processing time for this batch: {end_time - start_time} seconds.")

    if final_faulty:
        print(f"Faulty URLs: {len(final_faulty)}")
        save_to_json(final_faulty, output_dir=faulty_output_dir, save_name="NEW_faulty")
    if final_result:
        print(f"Data crawled success: {len(final_result)}")
        save_to_json(final_result, output_dir=result_output_dir, save_name="NEW_result")


##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

if __name__ == "__main__":
    main()
