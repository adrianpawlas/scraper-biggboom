#!/usr/bin/env python3
"""
Bigg Boom Scraper - Smart product management with batch inserts, upsert logic, and stale product handling.

Usage:
    python3 run.py [--skip-scrape] [--skip-embed] [--dry-run]
"""

import sys
import os
import re
import json
import time
import argparse
import logging
import threading
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqawmzggcgpeyaaynrjk.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

BATCH_SIZE = 50
EMBEDDING_DELAY = 0.5
MAX_RETRIES = 3
STALE_THRESHOLD_RUNS = 2

products_lock = threading.Lock()
last_seen_products: set = set()
previous_products: set = set()
stash_file = "output/.previous_products.json"


def load_previous_products() -> set:
    """Load product URLs from previous run"""
    if os.path.exists(stash_file):
        try:
            with open(stash_file, "r") as f:
                data = json.load(f)
                return set(data.get("product_urls", []))
        except Exception:
            pass
    return set()


def save_previous_products(product_urls: set):
    """Save product URLs for stale detection"""
    os.makedirs("output", exist_ok=True)
    with open(stash_file, "w") as f:
        json.dump({"product_urls": list(product_urls), "run_date": datetime.now().isoformat()}, f)


def extract_price(price_text: str) -> str:
    if not price_text:
        return ""
    numbers = re.findall(r"[\d,]+\.?\d*", price_text)
    if numbers:
        return f"{numbers[0].replace(',', '')}USD"
    return price_text


class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url
        self.key = key
        self.client_err = None

        try:
            from supabase import create_client
            self.supabase = create_client(url, key)
            self.use_lib = True
        except Exception as e:
            logger.warning(f"Supabase library not available, using REST API: {e}")
            import httpx
            self.http = httpx.Client(base_url=url, headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            }, timeout=30.0)
            self.use_lib = False

    def get_products_by_source(self, source: str) -> List[Dict]:
        """Get all products for a source"""
        if self.use_lib:
            try:
                result = self.supabase.table("products").select("*").eq("source", source).execute()
                return result.data or []
            except Exception as e:
                logger.error(f"Error fetching products: {e}")
                return []
        else:
            try:
                resp = self.http.get(f"/rest/v1/products?source=eq.{source}&select=*")
                if resp.status_code == 200:
                    return resp.json()
                return []
            except Exception as e:
                logger.error(f"Error fetching products: {e}")
                return []

    def batch_upsert(self, records: List[Dict], retry_count: int = 0) -> Tuple[int, int]:
        """Batch upsert products - returns (success_count, error_count)"""
        if not records:
            return 0, 0

        if self.use_lib:
            try:
                result = self.supabase.table("products").upsert(
                    records,
                    on_conflict="source,product_url",
                    ignore_duplicates=False
                ).execute()
                return len(records), 0
            except Exception as e:
                logger.error(f"Batch upsert error: {e}")
        else:
            try:
                resp = self.http.post(
                    "/rest/v1/products",
                    json=records
                )
                if resp.status_code in [200, 201]:
                    return len(records), 0
                elif resp.status_code in [500, 502, 503] and retry_count < MAX_RETRIES:
                    logger.warning(f"Retry {retry_count + 1}/{MAX_RETRIES}")
                    time.sleep(2 ** retry_count)
                    return self.batch_upsert(records, retry_count + 1)
                else:
                    return 0, len(records)
            except Exception as e:
                logger.error(f"Batch upsert error: {e}")

        return 0, len(records)

    def delete_products(self, product_ids: List[str]) -> int:
        """Delete products by IDs"""
        if not product_ids:
            return 0

        if self.use_lib:
            try:
                result = self.supabase.table("products").delete().in_("id", product_ids).execute()
                return len(product_ids)
            except Exception as e:
                logger.error(f"Delete error: {e}")
                return 0
        else:
            try:
                ids_str = ",".join([f'"{pid}"' for pid in product_ids])
                resp = self.http.request(
                    "DELETE",
                    f"/rest/v1/products?id=in.({ids_str})",
                    headers={"Prefer": "return=minimal"}
                )
                return len(product_ids) if resp.status_code in [200, 204] else 0
            except Exception as e:
                logger.error(f"Delete error: {e}")
                return 0


def run_scraper(output_dir="output"):
    """Run the scraper to collect all products"""
    import httpx
    from bs4 import BeautifulSoup

    BASE_URL = "https://biggboom.co"
    SHOP_URL = f"{BASE_URL}/shop/"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    CATEGORIES = [
        "https://biggboom.co/product-category/oversized-t-shirts/",
        "https://biggboom.co/product-category/hoodies/",
        "https://biggboom.co/product-category/caps/",
        "https://biggboom.co/product-category/long-sleeve-t-shirts/",
    ]

    logger.info("Starting scraper...")
    client = httpx.Client(timeout=60.0, follow_redirects=True)
    all_urls = set()

    for page in range(1, 10):
        url = f"{SHOP_URL}page/{page}/" if page > 1 else SHOP_URL
        r = client.get(url, headers=HEADERS)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "lxml")
        products = soup.select("li.product a")
        if not products:
            break
        for p in products:
            href = p.get("href")
            if href and "/shop/" in href:
                all_urls.add(href)
        logger.info(f"  Page {page}: {len(products)} products")
        time.sleep(0.5)

    for cat_url in CATEGORIES:
        cat_name = cat_url.split("/")[-2]
        for page in range(1, 10):
            url = f"{cat_url}page/{page}/" if page > 1 else cat_url
            r = client.get(url, headers=HEADERS)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")
            products = soup.select("li.product a")
            if not products:
                break
            for p in products:
                href = p.get("href")
                if href and "/shop/" in href:
                    all_urls.add(href)
            time.sleep(0.3)
        logger.info(f"  {cat_name}: total URLs: {len(all_urls)}")

    urls = list(all_urls)
    logger.info(f"Total unique product URLs: {len(urls)}")

    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/product_urls.txt", "w") as f:
        for url in urls:
            f.write(url + "\n")

    results = []
    for i, url in enumerate(urls):
        logger.info(f"Parsing {i+1}/{len(urls)}: {url[:60]}...")
        try:
            r = client.get(url, headers=HEADERS)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "lxml")

            title = soup.select_one("h1.product_title")
            title = title.get_text(strip=True) if title else None

            price_container = soup.select_one("p.price")
            original_price = None
            sale_price = None

            if price_container:
                del_el = price_container.select_one("del")
                if del_el:
                    original_price = del_el.get_text(strip=True)
                    original_price = extract_price(original_price)

                ins_el = price_container.select_one("ins")
                if ins_el:
                    sale_price = ins_el.get_text(strip=True)
                    sale_price = extract_price(sale_price)
                else:
                    sale_price = original_price

            main_image = soup.select_one("div.images img")
            image_url = main_image.get("src") if main_image else None

            additional_images = []
            gallery = soup.select("div.thumbnails a")
            for img_link in gallery:
                img_src = img_link.get("href") or img_link.select_one("img", {}).get("src")
                if img_src and img_src != image_url:
                    additional_images.append(img_src)

            category = soup.select_one("span.single-product-category a")
            category_text = category.get_text(strip=True) if category else None

            description = soup.select_one("div.woocommerce-product-details__short-description")
            description = description.get_text(strip=True) if description else None

            sizes = []
            size_options = soup.select("select#pa_size option")
            for size_opt in size_options[1:]:
                sizes.append(size_opt.get_text(strip=True))

            meta = {
                "title": title,
                "description": description,
                "sizes": sizes,
                "original_price_usd": original_price,
                "sale_price_usd": sale_price,
            }

            price_str = original_price if original_price else ""

            product = {
                "product_url": url,
                "title": title,
                "image_url": image_url,
                "additional_images": ", ".join(additional_images) if additional_images else None,
                "category": category_text,
                "description": description,
                "size": ", ".join(sizes) if sizes else None,
                "gender": None,
                "metadata": json.dumps(meta),
                "price": price_str,
                "sale": sale_price if sale_price != original_price else None,
                "source": "scraper-biggboom",
                "brand": "Bigg Boom",
                "second_hand": False,
            }
            results.append(product)

        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
        time.sleep(0.3)

    client.close()

    with open(f"{output_dir}/products.json", "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Products saved to {output_dir}/products.json ({len(results)} products)")

    return results


def run_embeddings(products_path="output/products.json", output_dir="output", dry_run: bool = False):
    """Generate embeddings and import to Supabase with smart upsert logic"""
    import httpx as hp
    import io
    from PIL import Image
    import torch
    from transformers import AutoProcessor, AutoModel

    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    MODEL_NAME = "google/siglip-base-patch16-384"

    logger.info("Creating embeddings...")

    logger.info(f"Loading model: {MODEL_NAME}...")
    processor = AutoProcessor.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()
    logger.info(f"Model loaded. Using device: {device}")

    def download_image(url: str):
        try:
            response = hp.get(url, timeout=30.0, follow_redirects=True)
            if response.status_code == 200:
                return Image.open(io.BytesIO(response.content)).convert("RGB")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
        return None

    def resize_image_for_model(image: Image.Image, target_size: int = 384) -> Image.Image:
        w, h = image.size
        if w == h:
            return image.resize((target_size, target_size), Image.LANCZOS)

        if w > h:
            new_w = target_size
            new_h = int(h * (target_size / w))
        else:
            new_h = target_size
            new_w = int(w * (target_size / h))

        image = image.resize((new_w, new_h), Image.LANCZOS)

        new_image = Image.new("RGB", (target_size, target_size), (255, 255, 255))
        paste_x = (target_size - new_w) // 2
        paste_y = (target_size - new_h) // 2
        new_image.paste(image, (paste_x, paste_y))

        return new_image

    def get_image_embedding(image: Image.Image):
        image = resize_image_for_model(image)
        inputs = processor(images=image, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.get_image_features(**inputs)

        return outputs.pooler_output.cpu().numpy().flatten().tolist()

    def get_text_embedding(text: str):
        text = text[:200]
        inputs = processor(text=text, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.get_text_features(**inputs)

        return outputs.pooler_output.cpu().numpy().flatten().tolist()

    def format_for_supabase(embedding):
        return "[" + ",".join(map(str, embedding)) + "]"

    with open(products_path, "r") as f:
        products = json.load(f)

    logger.info(f"Loaded {len(products)} products from scrape")

    prev_products = load_previous_products()
    current_product_urls = {p.get("product_url") for p in products}
    save_previous_products(current_product_urls)

    logger.info("Fetching existing products from Supabase...")
    client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    existing_products = client.get_products_by_source("scraper-biggboom")
    existing_by_url = {p.get("product_url"): p for p in existing_products}
    logger.info(f"Found {len(existing_products)} existing products in database")

    processed_records = []
    new_count = 0
    updated_count = 0
    unchanged_count = 0
    skipped_for_embed = 0

    http_client = hp.Client(timeout=30.0)

    for i, product in enumerate(products):
        url = product.get("product_url", "")
        title = product.get("title", "Unknown")
        logger.info(f"Processing {i+1}/{len(products)}: {title[:40]}...")

        existing = existing_by_url.get(url)
        product_id = url.split("/")[-2] if url else f"prod_{i}"
        product_id = f"biggboom_{product_id}"

        need_image_embed = False
        need_text_embed = False

        if existing is None:
            logger.info(f"  NEW product")
            new_count += 1
            need_image_embed = True
            need_text_embed = True
        else:
            has_changed = False
            if product.get("title") != existing.get("title"):
                has_changed = True
            if product.get("price") != existing.get("price"):
                has_changed = True
            if product.get("sale") != existing.get("sale"):
                has_changed = True
            if product.get("description") != existing.get("description"):
                has_changed = True

            old_image = existing.get("image_url", "")
            new_image = product.get("image_url", "")
            if old_image != new_image:
                has_changed = True
                need_image_embed = True

            if has_changed:
                logger.info(f"  UPDATED (data changed)")
                updated_count += 1
                need_image_embed = True
                need_text_embed = True
            else:
                logger.info(f"  UNCHANGED (skipping)")
                unchanged_count += 1

        image_embedding = None
        info_embedding = None

        if not dry_run:
            if need_image_embed or need_text_embed:
                image_url = product.get("image_url")
                if image_url and image_url.startswith("http"):
                    img = download_image(image_url)
                    if img:
                        image_embedding = get_image_embedding(img)
                        logger.info(f"  Image embedding: {len(image_embedding)} dims")
                        time.sleep(EMBEDDING_DELAY)

                if need_text_embed and product.get("title"):
                    text_info = f"{product.get('title', '')} {product.get('description', '')} {product.get('category', '')} {product.get('price', '')}"
                    text_info = text_info[:200]
                    info_embedding = get_text_embedding(text_info)
                    logger.info(f"  Text embedding: {len(info_embedding)} dims")
                    time.sleep(EMBEDDING_DELAY)
            else:
                image_embedding = existing.get("image_embedding")
                info_embedding = existing.get("info_embedding")
                skipped_for_embed += 1

        additional_images = product.get("additional_images")

        record = {
            "id": product_id,
            "source": product.get("source", "scraper-biggboom"),
            "product_url": url,
            "image_url": product.get("image_url"),
            "brand": product.get("brand", "Bigg Boom"),
            "title": product.get("title"),
            "description": product.get("description"),
            "category": product.get("category"),
            "gender": product.get("gender"),
            "metadata": product.get("metadata"),
            "size": product.get("size"),
            "second_hand": False,
            "country": "IN",
            "additional_images": additional_images,
            "price": product.get("price"),
            "sale": product.get("sale"),
            "updated_at": datetime.now().isoformat(),
        }

        if image_embedding:
            record["image_embedding"] = format_for_supabase(image_embedding)
        elif existing and existing.get("image_embedding"):
            record["image_embedding"] = existing.get("image_embedding")

        if info_embedding:
            record["info_embedding"] = format_for_supabase(info_embedding)
        elif existing and existing.get("info_embedding"):
            record["info_embedding"] = existing.get("info_embedding")

        processed_records.append(record)

    http_client.close()

    logger.info(f"\nProcessed: {new_count} new, {updated_count} updated, {unchanged_count} unchanged")
    logger.info(f"Skipped embedding regeneration: {skipped_for_embed}")

    stale_urls = prev_products - current_product_urls
    stale_count = 0
    if stale_urls and prev_products:
        logger.info(f"\nChecking for stale products ({len(stale_urls)} not in current scrape)...")

        stale_product_ids = []
        for url in stale_urls:
            if url in existing_by_url:
                stale_product_ids.append(existing_by_url[url].get("id"))

        if stale_product_ids:
            if not dry_run:
                deleted = client.delete_products(stale_product_ids)
                stale_count = deleted
                logger.info(f"Deleted {deleted} stale products")
            else:
                logger.info(f"Would delete {len(stale_product_ids)} stale products (dry run)")

    if dry_run:
        logger.info("\n=== DRY RUN - No database changes ===")
        return new_count, updated_count, unchanged_count, stale_count

    logger.info(f"\nBatch inserting {len(processed_records)} products...")

    success_total = 0
    error_total = 0
    for i in range(0, len(processed_records), BATCH_SIZE):
        batch = processed_records[i:i + BATCH_SIZE]
        logger.info(f"  Batch {i//BATCH_SIZE + 1}: {len(batch)} products...")
        success, errors = client.batch_upsert(batch)
        success_total += success
        error_total += errors
        if errors > 0:
            logger.error(f"    {errors} failed in batch")

    logger.info(f"\nImport complete: {success_total} success, {error_total} errors")

    with open(f"{output_dir}/import_log.txt", "a") as f:
        f.write(f"\n{datetime.now().isoformat()}: {new_count} new, {updated_count} updated, {unchanged_count} unchanged, {stale_count} deleted\n")

    return new_count, updated_count, unchanged_count, stale_count


def main():
    parser = argparse.ArgumentParser(description="Bigg Boom Scraper")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, just do embeddings")
    parser.add_argument("--skip-embed", action="store_true", help="Skip embeddings, just do scrape")
    parser.add_argument("--dry-run", action="store_true", help="Dry run - don't import to Supabase")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Bigg Boom Scraper Starting")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    products = None

    if not args.skip_scrape:
        products = run_scraper(args.output_dir)
    else:
        products_path = f"{args.output_dir}/products.json"
        if os.path.exists(products_path):
            with open(products_path, "r") as f:
                products = json.load(f)
            logger.info(f"Loaded {len(products)} products from cache")

    summary = (0, 0, 0, 0)

    if not args.skip_embed:
        summary = run_embeddings(
            products_path=f"{args.output_dir}/products.json",
            output_dir=args.output_dir,
            dry_run=args.dry_run
        )

    new_count, updated_count, unchanged_count, stale_count = summary

    logger.info("\n" + "=" * 60)
    logger.info("RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  {new_count} new products added")
    logger.info(f"  {updated_count} products updated")
    logger.info(f"  {unchanged_count} products unchanged (skipped)")
    logger.info(f"  {stale_count} stale products deleted")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("Dry run complete - no database changes")


if __name__ == "__main__":
    main()