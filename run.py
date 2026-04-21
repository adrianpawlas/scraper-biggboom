#!/usr/bin/env python3
"""
Bigg Boom Scraper - Main entry point for both manual and automated runs.
Can be run with: python3 run.py [--skip-scrape] [--skip-embed] [--dry-run]
"""

import sys
import os
import re
import json
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)
logger = logging.getLogger(__name__)

SUPABASE_URL = "https://yqawmzggcgpeyaaynrjk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"


def extract_price(price_text: str) -> str:
    if not price_text:
        return ""
    numbers = re.findall(r"[\d,]+\.?\d*", price_text)
    if numbers:
        return f"{numbers[0].replace(',', '')}USD"
    return price_text


def run_scraper(output_dir="output"):
    """Run the scraper to collect all products"""
    import httpx
    import time
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
            if original_price and sale_price:
                price_str = original_price

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


def run_embeddings(products_path="output/products.json", output_dir="output"):
    """Generate embeddings and import to Supabase"""
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

    logger.info(f"Loaded {len(products)} products")

    processed = []
    http_client = hp.Client(timeout=30.0)

    for i, product in enumerate(products):
        logger.info(f"Processing {i+1}/{len(products)}: {product.get('title', 'Unknown')[:40]}...")

        image_url = product.get("image_url")
        image_embedding = None
        info_embedding = None

        if image_url and image_url.startswith("http"):
            img = download_image(image_url)
            if img:
                image_embedding = get_image_embedding(img)
                logger.info(f"  Got image embedding: {len(image_embedding)} dims")

        if image_embedding and product.get("title"):
            text_info = f"{product.get('title', '')} {product.get('description', '')} {product.get('category', '')} {product.get('price', '')}"
            text_info = text_info[:200]
            info_embedding = get_text_embedding(text_info)
            logger.info(f"  Got text embedding: {len(info_embedding)} dims")

        additional_images = product.get("additional_images")

        product_id = product.get("product_url", "").split("/")[-2] if product.get("product_url") else f"prod_{i}"

        record = {
            "id": f"biggboom_{product_id}",
            "source": product.get("source", "scraper-biggboom"),
            "product_url": product.get("product_url"),
            "image_url": image_url,
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
            "created_at": "now()",
        }

        if image_embedding:
            record["image_embedding"] = format_for_supabase(image_embedding)

        if info_embedding:
            record["info_embedding"] = format_for_supabase(info_embedding)

        processed.append(record)
        logger.info(f"  Record prepared")

    http_client.close()

    logger.info(f"\nTotal records: {len(processed)}")
    logger.info("\nImporting to Supabase...")

    client = hp.Client(
        base_url=SUPABASE_URL,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
    )

    success_count = 0
    error_count = 0
    errors = []

    for record in processed:
        try:
            response = client.post(
                "/rest/v1/products",
                json=[record]
            )

            if response.status_code in [200, 201]:
                success_count += 1
                logger.info(f"Imported: {record.get('title', '')[:40]}")
            else:
                error_count += 1
                err_msg = f"Error importing {record.get('title', '')[:40]}: {response.status_code}"
                logger.error(err_msg)
                errors.append(err_msg)

        except Exception as e:
            error_count += 1
            logger.error(f"Exception: {e}")

    client.close()

    logger.info(f"\nImport complete: {success_count} success, {error_count} errors")

    with open(f"{output_dir}/import_log.txt", "a") as f:
        f.write(f"\n{datetime.now().isoformat()}: {success_count} success, {error_count} errors\n")
        if errors:
            for e in errors:
                f.write(f"  {e}\n")

    return success_count, error_count


def main():
    parser = argparse.ArgumentParser(description="Bigg Boom Scraper")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, just do embeddings")
    parser.add_argument("--skip-embed", action="store_true", help="Skip embeddings, just do scrape")
    parser.add_argument("--dry-run", action="store_true", help="Dry run - don't import to Supabase")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Bigg Boom Scraper Starting")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 50)

    products = None

    if not args.skip_scrape:
        products = run_scraper(args.output_dir)
    else:
        products_path = f"{args.output_dir}/products.json"
        if os.path.exists(products_path):
            with open(products_path, "r") as f:
                products = json.load(f)
            logger.info(f"Loaded {len(products)} products from cache")

    if not args.skip_embed and not args.dry_run:
        success, errors = run_embeddings(
            products_path=f"{args.output_dir}/products.json",
            output_dir=args.output_dir
        )

        if errors > 0:
            logger.warning(f"Import had {errors} errors - please check logs")
            sys.exit(1)
    elif args.dry_run:
        logger.info("Dry run - skipping Supabase import")

    logger.info("=" * 50)
    logger.info("Scraper Complete!")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()