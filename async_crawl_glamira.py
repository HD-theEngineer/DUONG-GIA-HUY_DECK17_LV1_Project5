import asyncio
import aiohttp
import json
import random
import time
from lxml import html

MAX_CONCURRENCY = 20
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2
RETRY_BACKOFF_FACTOR = 2

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
]

HEADERS_TEMPLATE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://glamira.vn/",
}


async def fetch(session, sem, data, retry_count=0):
    url = data.get("url")
    headers = {**HEADERS_TEMPLATE, "User-Agent": random.choice(USER_AGENTS)}
    try:
        async with sem:
            async with session.get(
                url, headers=headers, timeout=REQUEST_TIMEOUT
            ) as resp:
                status = resp.status
                text = await resp.text(errors="ignore")
                if status in [200, 201]:
                    tree = html.fromstring(text)
                    # Example: get <h1 class="page-title"> text
                    title = tree.xpath('//h1[contains(@class, "page-title")]/text()')
                    title_text = title[0].strip() if title else "No title found"
                    return {
                        "status": "success",
                        "id": data.get("id"),
                        "title": title_text,
                        "url": url,
                    }
                elif (
                    status in [403, 429, 500, 502, 503, 504]
                    and retry_count < MAX_RETRIES
                ):
                    await asyncio.sleep(
                        random.uniform(0.5, 1.5)
                        * (RETRY_BACKOFF_FACTOR * (retry_count + 1))
                    )
                    return await fetch(session, sem, data, retry_count + 1)
                else:
                    return {
                        "status": "failed",
                        "id": data.get("id"),
                        "reason": f"Status code {status}",
                        "url": url,
                    }
    except Exception as e:
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(
                random.uniform(0.5, 1.5) * (RETRY_BACKOFF_FACTOR * (retry_count + 1))
            )
            return await fetch(session, sem, data, retry_count + 1)
        return {
            "status": "failed",
            "id": data.get("id"),
            "reason": str(e),
            "url": url,
        }


async def batch_crawl_from_url(data_batch):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, sem, data) for data in data_batch]
        results = await asyncio.gather(*tasks)
    result = [r for r in results if r["status"] == "success"]
    faulty_package = [r for r in results if r["status"] != "success"]
    return result, faulty_package


def load_batches(file_pool, batch_size):
    from json_processing import streaming_json, batching_data

    package = streaming_json(file_pool)
    for batch in batching_data(package, batch_size):
        yield batch


def main():
    import time

    file_pool = [
        "view_product_detail_L.json",
        # Add more files as needed
    ]
    file_path = "D:\\glamira-data\\"
    name_pool = [file_path + file for file in file_pool]
    batch_num = 0
    batch_size = 1000

    for batch in load_batches(name_pool, batch_size):
        batch_num += 1
        start_time = time.perf_counter()
        result, faulty_package = asyncio.run(batch_crawl_from_url(batch))
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


if __name__ == "__main__":
    main()
