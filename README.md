# Bigg Boom Scraper

Scraper for the Bigg Boom fashion store (https://biggboom.co).

## Features

- Scrapes all products from the Bigg Boom online store
- Generates 768-dim image embeddings using SigLIP-base-patch16-384
- Generates 768-dim text embeddings for product info
- Imports directly to Supabase

## Usage

### Local Run

```bash
# Full scrape and import
python3 run.py

# Skip scrape, just do embeddings (use cached data)
python3 run.py --skip-scrape

# Skip embeddings, just scrape
python3 run.py --skip-embed

# Dry run (don't import to Supabase)
python3 run.py --dry-run
```

### GitHub Actions

The scraper runs automatically every Saturday at midnight via GitHub Actions.

**Manual Run:**
1. Go to Actions tab
2. Select "Bigg Boom Scraper"
3. Click "Run workflow"
4. Optional: Use input flags to customize

## Output

- `output/products.json` - All scraped product data
- `output/product_urls.txt` - All product URLs
- `output/import_log.txt` - Import logs

## Supabase Fields

| Field | Description |
|-------|------------|
| id | Unique ID (biggboom_{slug}) |
| source | "scraper-biggboom" |
| brand | "Bigg Boom" |
| image_embedding | 768-dim SigLIP image embedding |
| info_embedding | 768-dim SigLIP text embedding |
| title | Product name |
| description | Product description |
| category | Product category |
| price | Original price (USD) |
| sale | Sale price (USD) |
| gender | null (unisex products) |
| second_hand | false |
| metadata | JSON with sizes, colors, etc. |

## Requirements

- Python 3.12+
- httpx
- beautifulsoup4
- lxml
- transformers
- torch
- pillow

Install via: `pip install -r requirements.txt`