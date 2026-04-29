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
]


async def fetch_and_parse(item, session, semaphore, max_retries=2):
    pid = item["product_id"]
    original_url = item["url"]

    domain = "www.glamira.com"
    if original_url:
        try:
            parsed_uri = urlparse(original_url)
            if parsed_uri.netloc:
                domain = parsed_uri.netloc
        except Exception:
            pass

    last_status = "Unknown"
    last_url = ""

    for attempt in range(max_retries + 1):
        if attempt == 0:
            target_url = f"https://www.glamira.sg/catalog/product/view/id/{pid}"
            tag = "[SG-STORE]"
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
                        "Accept-Language": "en-US,en;q=0.9",
                        "Referer": f"https://{domain}/",
                    }
                )

                if response.status_code == 200:
                    parsed_data = parse_react_data(response.text, pid, target_url)
                    logger.info(f"ID: {pid} | Success at {tag} | Name: {parsed_data.get('name')[:30]}...")
                    return parsed_data

                elif response.status_code == 404:
                    last_status = "Not Found (404)"
                    logger.warning(f"ID: {pid} | 404 Not Found at {tag}")
                    continue
                else:
                    last_status = f"Status {response.status_code}"
                    logger.warning(f"ID: {pid} | Failed: Status {response.status_code} at {tag}")
                    continue

            except Exception as e:
                last_status = f"Error: {str(e)[:20]}"
                logger.error(f"ID: {pid} | Connection error at {tag}: {str(e)[:30]}")
                continue

    logger.warning(f"ID: {pid} | FAILED after {max_retries} tries. Last status: {last_status}")
    return {"product_id": pid, "status": f"Failed ({last_status})", "url": last_url}


BROWSER_PROFILES = ["chrome110", "chrome116", "chrome120", "edge101", "safari15_3", "safari15_5"]


async def fetch_and_parse_slow(item, semaphore):
    pid = item["product_id"]
    original_url = item["url"]

    domain = "www.glamira.com"
    if original_url:
        try:
            parsed_uri = urlparse(original_url)
            if parsed_uri.netloc:
                domain = parsed_uri.netloc
        except Exception:
            pass

    target_url = f"https://{domain}/catalog/product/view/id/{pid}"
    tag = f"[SLOW-RETRY-{domain.upper()}]"

    for attempt in range(3):
        async with semaphore:
            async with rate_limiter:
                profile = random.choice(PROFILES)

                try:
                    async with AsyncSession(impersonate=profile["name"]) as session:
                        response = await session.get(
                            target_url,
                            headers={
                                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                                "Accept-Language": "en-US,en;q=0.9",
                                "Referer": f"https://{domain}/",
                                "Upgrade-Insecure-Requests": "1"
                            },
                            timeout=60
                        )

                        if response.status_code == 200:
                            parsed_data = parse_react_data(response.text, pid, target_url)
                            logger.info(f"ID: {pid} | Success at {tag} | Name: {parsed_data.get('name')[:30]}...")
                            return parsed_data

                        elif response.status_code in [403, 429]:
                            logger.warning(f"ID: {pid} | Rate Limit {response.status_code} | Thử lại lần {attempt + 1}")
                        else:
                            return {"product_id": pid, "status": f"Status {response.status_code}", "url": original_url}

                except Exception as e:
                    logger.error(f"ID: {pid} | Lỗi kết nối: {str(e)[:50]}")

        sleep_time = (2 * (2 ** attempt)) + random.uniform(1, 5)
        await asyncio.sleep(sleep_time)

    return {"product_id": pid, "status": "Failed After Backoff", "url": original_url}


async def run_async_crawler(product_targets):
    CONCURRENT_REQUESTS = 10
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    logger.info("Start fast crawling phase 1...")
    async with AsyncSession() as session:
        tasks = [fetch_and_parse(item, session, semaphore) for item in product_targets]
        return await asyncio.gather(*tasks)

async def run_async_slow_crawler(product_targets):
    CONCURRENT_REQUESTS = 2
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    logger.info(f"Crawling 403 & 429 IDs again: Found {len(product_targets)} IDs...")
    tasks = [fetch_and_parse_slow(item, semaphore) for item in product_targets]
    return await asyncio.gather(*tasks)