import time
import logging
import IP2Location
from src.database import get_database
from src.config import IP2LOC_DB_PATH

logger = logging.getLogger("IPProcessor")

def process_ip_locations():
    start_time = time.time()
    db = get_database()
    summary_col = db["summary"]
    location_col = db["ip_locations"]

    location_col.drop()

    try:
        ip2loc_obj = IP2Location.IP2Location(IP2LOC_DB_PATH)
    except Exception as e:
        logger.error(f"Location file not found at {IP2LOC_DB_PATH}! Error: {e}")
        return

    logger.info("Filtering unique IPs")
    pipeline = [
        {"$match": {"ip": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$ip"}}
    ]
    cursor = summary_col.aggregate(pipeline, allowDiskUse=True)

    logger.info("Getting location data...")
    batch_size = 10000
    results = []
    total_inserted = 0

    for doc in cursor:
        ip = doc["_id"]
        try:
            rec = ip2loc_obj.get_all(ip)
            if rec and rec.country_long:
                results.append({
                    "ip": ip,
                    "country": rec.country_long,
                    "city": rec.city
                })
        except Exception:
            continue

        if len(results) >= batch_size:
            location_col.insert_many(results)
            total_inserted += len(results)
            logger.info(f"Saved {total_inserted} IPs...")
            results.clear()

    if results:
        location_col.insert_many(results)
        logger.info(f"Saved {len(results)} IPs (Collection: ip_locations)")

    logger.info(f"All finished in {round(time.time() - start_time, 2)} seconds!")