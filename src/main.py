import time
import argparse
import asyncio
import logging
import os
from pymongo import UpdateOne
from src.database import get_database, get_product_targets
from src.crawler.crawler import run_async_crawler, run_async_slow_crawler
from src.geo.ip_processor import process_ip_locations

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger("Main")


async def run_crawler():
    start_time = time.time()
    db = get_database()
    product_col = db["product_dictionary"]

    logger.info("Getting product urls from DB...")

    product_targets = get_product_targets(db)

    logger.info(f"Found {len(product_targets)} product IDs.")

    if not product_targets:
        logger.warning("No products found to crawl. Exiting...")
        return
    results1 = asyncio.run(run_async_crawler(product_targets))

    final_results = []
    retry_403_targets = []

    for res in results1:
        if "403" in res.get("status", "") or "429" in res.get("status", ""):
            retry_403_targets.append({"product_id": res["product_id"], "url": res["url"]})
        else:
            final_results.append(res)

    if retry_403_targets:
        await asyncio.sleep(60)
        results2 = asyncio.run(run_async_slow_crawler(retry_403_targets))
        final_results.extend(results2)

    logger.info("Storing data...")
    if final_results:
        operations = [
            UpdateOne(
                {"product_id": item["product_id"]},
                {"$set": item},
                upsert=True
            )
            for item in final_results
        ]

        result = product_col.bulk_write(operations)

        logger.info(f"DONE! Upserted {len(final_results)} products into 'product_dictionary'")
        logger.info(
            f"MongoDB detail: Matched {result.matched_count}, Modified {result.modified_count}, Inserted {result.upserted_count}")

    logger.info(f"Crawl Time: {round(time.time() - start_time, 2)} secs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glamira Data Pipeline")
    parser.add_argument("--job", choices=["crawl", "geo"], required=True,
                        help="Choose job: 'crawl' (get product information) or 'geo' (get location using IP)")

    args = parser.parse_args()

    if args.job == "crawl":
        run_crawler()
    elif args.job == "geo":
        process_ip_locations()