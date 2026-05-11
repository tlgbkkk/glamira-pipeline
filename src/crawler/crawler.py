import asyncio
import random
import logging
from urllib.parse import urlparse
from aiolimiter import AsyncLimiter
from curl_cffi.requests import AsyncSession
from src.crawler.parser import parse_react_data

rate_limiter = AsyncLimiter(1, 2)
logger = logging.getLogger("GlamiraCrawler")

PROFILES = [
    {"name": "chrome110", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"},
    {"name": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
    {"name": "edge101", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.47"},
    {"name": "safari15_3", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15"},
    {"name": "chrome116", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"},
]

ACCEPT_HEADERS = {
    "chrome": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "safari": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "edge":   "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
}

def _get_accept(profile_name: str) -> str:
    if "safari" in profile_name:
        return ACCEPT_HEADERS["safari"]
    if "edge" in profile_name:
        return ACCEPT_HEADERS["edge"]
    return ACCEPT_HEADERS["chrome"]

def _get_domain(item) -> str:
    try:
        parsed = urlparse(item.get("url", ""))
        return parsed.netloc or "www.glamira.com"
    except Exception:
        return "www.glamira.com"


async def fetch_and_parse(item, session, semaphore, max_retries=2):
    pid = item["product_id"]
    domain = _get_domain(item)

    last_status = "Unknown"
    last_url = ""

    for attempt in range(max_retries + 1):
        if attempt == 0:
            target_url = f"https://www.glamira.com/catalog/product/view/id/{pid}"
            tag = "[US-STORE]"
        else:
            target_url = f"https://{domain}/catalog/product/view/id/{pid}"
            tag = f"[{domain.upper()}]"

        last_url = target_url

        async with semaphore:
            sleep_time = random.uniform(0.5, 1.5) if attempt == 0 else random.uniform(2, 5)
            await asyncio.sleep(sleep_time)

            try:
                profile = random.choice(PROFILES)
                response = await session.get(
                    target_url,
                    impersonate=profile["name"],
                    headers={
                        "User-Agent": profile["ua"],
                        "Accept": _get_accept(profile["name"]),
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Referer": f"https://{domain}/",
                        "Cache-Control": "max-age=0",
                    },
                )

                if response.status_code == 200:
                    parsed_data = parse_react_data(response.text, pid, target_url)
                    logger.info(f"ID: {pid} | OK {tag} | {parsed_data.get('name','')[:30]}")
                    return parsed_data

                last_status = f"Status {response.status_code}" if response.status_code != 404 else "Not Found (404)"
                logger.warning(f"ID: {pid} | {last_status} at {tag}")

            except Exception as e:
                last_status = f"Error: {str(e)[:50]}"
                logger.error(f"ID: {pid} | Exception at {tag}: {last_status}")

    logger.warning(f"ID: {pid} | FAILED after all retries. Last: {last_status}")
    return {"product_id": pid, "status": last_status, "url": last_url}


async def fetch_and_parse_slow(item, semaphore):
    pid = item["product_id"]
    domain = _get_domain(item)
    original_url = item.get("url", "")

    target_url = f"https://{domain}/catalog/product/view/id/{pid}"
    tag = f"[SLOW-{domain.upper()}]"

    last_status = "Unknown"

    for attempt in range(3):
        async with semaphore:
            async with rate_limiter:
                profile = random.choice(PROFILES)

                try:
                    async with AsyncSession(impersonate=profile["name"]) as session:
                        response = await session.get(
                            target_url,
                            headers={
                                "User-Agent": profile["ua"],
                                "Accept": _get_accept(profile["name"]),
                                "Accept-Language": "en-US,en;q=0.9",
                                "Accept-Encoding": "gzip, deflate, br",
                                "Referer": f"https://{domain}/",
                                "Upgrade-Insecure-Requests": "1",
                                "Sec-Fetch-Dest": "document",
                                "Sec-Fetch-Mode": "navigate",
                                "Sec-Fetch-Site": "same-origin",
                                "Sec-Fetch-User": "?1",
                            },
                            timeout=60,
                        )

                        code = response.status_code

                        if code == 200:
                            parsed_data = parse_react_data(response.text, pid, target_url)
                            logger.info(f"ID: {pid} | OK {tag} | {parsed_data.get('name','')[:30]}")
                            return parsed_data

                        if code == 404:
                            last_status = "Not Found (404)"
                            logger.warning(f"ID: {pid} | 404 at {tag}")
                            return {"product_id": pid, "status": last_status, "url": original_url}

                        if code in (403, 429):
                            last_status = f"Status {code}"
                            logger.warning(f"ID: {pid} | {code} rate-limited, attempt {attempt+1}/3")
                        else:
                            last_status = f"Status {code}"
                            logger.warning(f"ID: {pid} | Unexpected {code} at {tag}")
                            return {"product_id": pid, "status": last_status, "url": original_url}

                except Exception as e:
                    last_status = f"Error: {str(e)[:50]}"
                    logger.error(f"ID: {pid} | Exception: {last_status}")

        sleep_time = (2 ** (attempt + 1)) + random.uniform(1, 5)
        logger.info(f"ID: {pid} | Backoff {sleep_time:.1f}s before {attempt+2} tries")
        await asyncio.sleep(sleep_time)

    logger.warning(f"ID: {pid} | Failed after retries. Status: {last_status}")
    return {"product_id": pid, "status": last_status, "url": original_url}


async def run_async_crawler(product_targets):
    semaphore = asyncio.Semaphore(20)
    logger.info("Phase 1 (fast)...")
    async with AsyncSession() as session:
        tasks = [fetch_and_parse(item, session, semaphore) for item in product_targets]
        return await asyncio.gather(*tasks)


async def run_async_slow_crawler(product_targets):
    CONCURRENT_REQUESTS = 5
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    BATCH_SIZE = 100
    WAIT_MINUTES = 15

    batches = [product_targets[i:i+BATCH_SIZE] for i in range(0, len(product_targets), BATCH_SIZE)]
    total = len(batches)
    all_results = []

    for idx, batch in enumerate(batches):
        logger.info(f"Batch {idx+1}/{total}: {len(batch)} IDs...")
        tasks = [fetch_and_parse_slow(item, semaphore) for item in batch]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)

        if idx < total - 1:
            logger.info(f"Wait {WAIT_MINUTES} before batch {idx+2}/{total}...")
            await asyncio.sleep(WAIT_MINUTES * 60)

    logger.info(f"Done: {len(all_results)} IDs")
    return all_results