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

    logger.info("=== Phase 1: Fast crawl ===")
    results1 = await run_async_crawler(product_targets)

    final_results = []
    retry_targets = []

    for res in results1:
        status = res.get("status", "")
        if "403" in status or "429" in status or "Unknown" in status:
            retry_targets.append({"product_id": res["product_id"], "url": res["url"]})
        else:
            final_results.append(res)

    logger.info(f"Phase 1: {len(final_results)} OK, {len(retry_targets)} cần retry")

    if retry_targets:
        logger.info(f"=== Phase 2: Retry {len(retry_targets)} IDs ===")
        await asyncio.sleep(60)
        results2 = await run_async_slow_crawler(retry_targets)

        still_failed = []
        for res in results2:
            status = res.get("status", "")
            if "403" in status or "Unknown" in status:
                still_failed.append(res)
            final_results.append(res)

        if still_failed:
            logger.warning(f"{len(still_failed)} failed IDs found!")

    logger.info(f"Storing {len(final_results)} records...")
    if final_results:
        operations = [
            UpdateOne(
                {"product_id": item["product_id"]},
                {"$set": item},
                upsert=True
            )
            for item in final_results
            if item.get("name")
        ]

        if operations:
            result = product_col.bulk_write(operations)
            logger.info(f"DONE! Upserted {len(operations)} products vào 'product_dictionary'")
            logger.info(f"MongoDB: Matched {result.matched_count}, Modified {result.modified_count}, Inserted {result.upserted_count}")
        else:
            logger.warning("No valid records")

    logger.info(f"Crawl Time: {round(time.time() - start_time, 2)} secs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glamira Data Pipeline")
    parser.add_argument("--job", choices=["crawl", "geo"], required=True,
                        help="Choose job: 'crawl' or 'geo'")
    args = parser.parse_args()

    if args.job == "crawl":
        asyncio.run(run_crawler())
    elif args.job == "geo":
        process_ip_locations()