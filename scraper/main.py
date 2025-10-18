import re
import asyncio
import random
import pandas as pd
from typing import Optional, Dict, Any, List
from tqdm import tqdm
from playwright.async_api import (
    async_playwright,
    Page,
    BrowserContext,
    TimeoutError as PwTimeoutError,
)

# PROVINCIAS = ["san-jose", "alajuela", "cartago", "heredia", "guanacaste", "puntarenas", "limon"]
PROVINCIAS = ["san-jose"]


START_URL = "https://www.encuentra24.com/costa-rica-es/searchresult/bienes-raices-venta-de-propiedades-lotes-y-terrenos?regionslug={provincia}-provincia"
PAGE_URL  = "https://www.encuentra24.com/costa-rica-es/searchresult/bienes-raices-venta-de-propiedades-lotes-y-terrenos.{p}?regionslug={provincia}-provincia"
BASE = "https://encuentra24.com"
CONCURRENCY = 5

# ---------------- helpers ----------------

def _clean(s: Optional[str]) -> str:
    if not s:
        return ""
    return (
        s.strip()
        .replace(",", "\\,")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\"", "\\\"")
    )

async def _inner_text_opt(locator) -> str:
    try:
        return await locator.inner_text()
    except Exception:
        return ""

def _looks_like_listing(href: str) -> bool:
    if not href or not href.startswith("/"):
        return False
    if href.startswith(("/ad-click", "/click", "/track")):
        return False
    if "#" in href or "javascript:" in href:
        return False
    return href.startswith("/costa-rica-es/bienes-raices-")

# ------------- extract with timeout/retries -------------

async def extract_data_once(context: BrowserContext, url: str, provincia: str) -> Optional[Dict[str, Any]]:
    page: Page = await context.new_page()
    try:
        full_url = f"{BASE}{url}"
        await page.goto(full_url, wait_until="domcontentloaded")

        area_raw = await _inner_text_opt(page.get_by_text(re.compile(r"Tamaño del Lote m²\s*\d+")))
        area = area_raw.replace("Tamaño del Lote m²", "").strip()

        location_raw = await _inner_text_opt(page.get_by_text(re.compile(r"Localización\s*(.*)")))
        location = _clean(location_raw.replace("Localización", "").strip())

        pub_raw = await _inner_text_opt(page.get_by_text(re.compile(r"Publicado\s*\d+")))
        publication_date = pub_raw.replace("Publicado", "").strip()

        price_raw = await _inner_text_opt(page.get_by_text(re.compile(r'^[\$₡]\s*\d+(?:[.,]\d+)?')))
        price = price_raw.strip().replace(",", "").replace(".", "")

        details_raw = await _inner_text_opt(page.locator(".d3-property-about__text"))
        details = _clean(details_raw)

        return {
            "provincia": provincia,
            "area": area,
            "location": location,
            "publication_date": publication_date,
            "price": price,
            "url": full_url,
            "details": details,
        }
    finally:
        await page.close()

async def extract_data(context: BrowserContext, url: str, provincia: str, timeout_s: float = 20.0, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            return await asyncio.wait_for(extract_data_once(context, url, provincia), timeout=timeout_s)
        except (PwTimeoutError, asyncio.TimeoutError) as e:
            if attempt < retries:
                await asyncio.sleep(0.5 + random.random())
                continue
            print(f"Timeout extracting {url}: {e}")
            return None
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(0.5 + random.random())
                continue
            print(f"extract_data failed for {url}: {e}")
            return None

# ---------------- per-page processing ----------------

async def process_page(
    page: Page,
    listings_data: List[Dict[str, Any]],
    context: BrowserContext,
    provincia: str,
    pnum: int,
    limit: Optional[int] = None
):
    urls: List[str] = await page.eval_on_selector_all(
        ".d3-ad-tile__description",
        "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
    )
    urls = [u for u in urls if _looks_like_listing(u)]

    if not urls:
        print(f"No listings found on {provincia} page {pnum}")
        return

    if limit is not None:
        urls = urls[:limit]

    sem = asyncio.Semaphore(CONCURRENCY)

    async def worker(u: str):
        async with sem:
            return await extract_data(context, u, provincia)

    tasks = [asyncio.create_task(worker(u)) for u in urls]
    desc = f"{provincia} p{pnum}: extracting"
    results: List[Dict[str, Any]] = []

    with tqdm(total=len(urls), desc=desc, unit="ad", leave=False) as pbar:
        for fut in asyncio.as_completed(tasks):
            try:
                res = await fut
                if res:
                    results.append(res)
            finally:
                pbar.update(1)

    listings_data.extend(results)

# ---------------- runner ----------------

async def run():
    listings_data: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        # optional: keep these; they are synchronous setters in async API
        context.set_default_timeout(8000)
        context.set_default_navigation_timeout(10000)

        # IMPORTANT: async route handler with awaits
        async def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "font", "stylesheet"}:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", route_handler)

        for provincia in PROVINCIAS:
            print(f"==================== Provincia: {provincia} ====================")
            page = await context.new_page()
            await page.goto(START_URL.format(provincia=provincia), wait_until="domcontentloaded")

            pages_list = await page.eval_on_selector_all(
                ".d3-pagination a",
                "els => els.map(e => e.getAttribute('data-page')).filter(Boolean).map(Number).filter(n => !Number.isNaN(n))"
            )
            pages = max(pages_list) if pages_list else 1

            # for pnum in range(1, pages + 1):
            for pnum in range(1, 2):
                print(f"Processing page {pnum}/{pages} for {provincia}")
                await page.goto(PAGE_URL.format(p=pnum, provincia=provincia), wait_until="domcontentloaded")
                await process_page(page, listings_data, context, provincia, pnum, limit=None)

            await page.close()

        await browser.close()

    df = pd.DataFrame(listings_data)
    df.to_csv("listings_data.csv", index=False)
    print(f"Saved {len(df)} records to listings_data.csv")

if __name__ == "__main__":
    asyncio.run(run())
