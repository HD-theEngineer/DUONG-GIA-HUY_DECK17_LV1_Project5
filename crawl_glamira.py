import json  # LIb to work with JSON file
import requests  # Lib to send HTTP request and receive HTML source code
import time  # Time lib to measure execution time
import random  # Randomizer lib to random the sleep time and randomly select user-agent
from bs4 import BeautifulSoup  # Lib to read HTML code
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)  # Lib to create multi-thread runs
from json_processing import stream_and_batch

MAX_WORKERS = 10
REQUEST_TIMEOUT = 10  # Tăng timeout lên một chút để xử lý các trang load chậm
MAX_RETRIES = 2  # Số lần thử lại tối đa cho một URL
RETRY_BACKOFF_FACTOR = 2  # Hệ số nhân cho thời gian chờ giữa các lần retry

##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


def souping_data(session, data=None, headers=None, retry_count=0):
    url = data.get("url")

    try:
        response = session.get(url, headers=headers, timeout=3)

        if response.status_code in [200, 201]:
            # short_url = re.sub(r'[\\/*?:"<>|]', "_", url[:100])
            # file_name = f"D:\\glamira-data\\html\\{short_url}.html"
            # # file_name = f"{short_url}.html"
            # # file_name = "sample.txt"
            # with open(file_name, "w", encoding="utf-8") as html_doc:
            #     html_doc.write(response.text)
            # print("HTML file saved.")
            # print("Success.")
            soup = BeautifulSoup(response.text, "html.parser")
            page_title = soup.find("h1", class_="page-title")
            title_text = (
                page_title.get_text(strip=True) if page_title else "No title found"
            )
            return {
                "status": "success",
                "id": data.get("id"),
                "title": title_text,
                "url": url,
            }
        elif response.status_code in [403, 429, 500, 502, 503, 504]:
            # print("403 occurred, will retry later.")
            return {
                "status": "retry",
                "data": data,
                "headers": headers,
                "retry_count": retry_count + 1,
            }
        else:
            # print(f"{response.status_code} occurred: {response.reason}.")
            return {
                "status": "failed",
                "id": data.get("id"),
                "reason": f"Status code {response.status_code}",
                "url": url,
            }
    except requests.exceptions.RequestException as e:
        # print(f"Exception occurred: {e}.\n")
        return {
            "status": "retry",
            "data": data,
            "headers": headers,
            "retry_count": retry_count + 1,
        }


def batch_crawl_from_url(data_batch=None, headers_template=None, user_agents=None):
    result = []
    faulty_package = []

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            try:
                for data in data_batch:
                    headers = {
                        **headers_template,
                        "User-Agent": random.choice(user_agents),
                    }
                    future = executor.submit(souping_data, session, data, headers)
                    futures[future] = data
            except Exception as e:
                print(f"Error during submission: {e}")
                return result, faulty_package

            while futures:
                done_futures = as_completed(futures)

                try:
                    # Take the next future in completed futures list
                    # and remove that from the list
                    future = next(done_futures)
                    original_data = futures.pop(future)

                    crawled_data = future.result()

                    if crawled_data.get("status") == "success":
                        result.append(crawled_data)
                    elif crawled_data.get("status") == "retry":
                        retry_count = crawled_data.get("retry_count")
                        if retry_count < MAX_RETRIES:
                            sleep_time = random.uniform(0.5, 1.5) * (
                                RETRY_BACKOFF_FACTOR * retry_count
                            )
                            time.sleep(sleep_time)

                            # retry submitting task
                            data_to_retry = crawled_data.get("data")
                            new_headers = crawled_data.get("headers")
                            new_future = executor.submit(
                                souping_data,
                                session,
                                data_to_retry,
                                new_headers,
                                retry_count,
                            )
                            futures[new_future] = data_to_retry
                        else:
                            # print(f"ERROR: Max retries reached for {crawled_data['data']['url']}. Giving up.")
                            faulty_package.append(
                                {
                                    "status": "failed",
                                    "id": crawled_data["data"].get("id"),
                                    "reason": "Max retries reached",
                                    "url": crawled_data["data"].get("url"),
                                }
                            )
                    elif crawled_data.get("status") == "failed":
                        faulty_package.append(crawled_data)

                except Exception as e:
                    original_data = futures[future]
                    print(f"Error processing: {e}")
                    faulty_package.append(
                        {
                            "status": "failed",
                            "id": original_data.get("id"),
                            "reason": str(e),
                            "url": original_data.get("url"),
                        }
                    )

    return result, faulty_package


##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def main():
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
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

    file_pool = [
        "view_product_detail_L.json",
        # "select_product_option_L.json",
        # "select_product_option_quality_L.json",
        # "add_to_cart_action.json",
        # "product_detail_recommendation_visible.json",
        # "product_detail_recommendation_noticed.json",
        # "product_view_all_recommend_clicked.json",
    ]
    file_path = "D:\\glamira-data\\"
    name_pool = [file_path + file for file in file_pool]
    batch_num = 0
    # batch_1000 = stream_and_batch(name_pool, 1000)
    for batch in stream_and_batch(name_pool, 1000):
        batch_num += 1
        start_time = time.perf_counter()

        result, faulty_package = batch_crawl_from_url(
            batch, headers_template, user_agents
        )

        end_time = time.perf_counter()

        if faulty_package:
            print(f"Faulty URLs: {len(faulty_package)}")
            with open(
                f"faulty_package_{batch_num}.json", "w", encoding="utf-8"
            ) as file:
                json.dump(faulty_package, file, ensure_ascii=False, indent=4)
        if result:
            print(f"Data crawled success: {len(result)}")
            with open(f"result_{batch_num}.json", "w", encoding="utf-8") as file:
                json.dump(result, file, ensure_ascii=False, indent=4)

        print(f"Processing time for this batch: {end_time - start_time} seconds.")


##''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

if __name__ == "__main__":
    main()
