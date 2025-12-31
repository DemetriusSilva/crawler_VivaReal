import glob
import os
import logging
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from google.cloud import storage

from viva_real.pipeline_full import run_full_pipeline
from viva_real.captura_links_async import VivaRealLinkScraper
from viva_real.scraper_async import VivaRealScraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def capturar_links_async(num_pages: int = 5, headless: bool = True, out_dir: str = "output", base_url: Optional[str] = None) -> str:
    links_path = Path(out_dir) / "links"
    links_path.mkdir(parents=True, exist_ok=True)
    links_dir = str(links_path)
    link_scraper = VivaRealLinkScraper(base_url=base_url, output_dir=links_dir, headless=headless)
    return await link_scraper.scrape_links(num_pages)

async def extrair_dados_link_async(link: str, headless: bool = True, out_dir: str = "output") -> bool:
    dados_path = Path(out_dir) / "dados"
    dados_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")
    csv_path = str(dados_path / f"{timestamp}_vivareal_single.csv")
    scraper = VivaRealScraper(csv_path=csv_path, headless=headless)
    return await scraper.scrape_link(link)

def upload_to_bucket(source_folder, bucket_name, destination_blob_folder):
    try:
        if not bucket_name: return
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        if not os.path.exists(source_folder): return
        
        files = glob.glob(f"{source_folder}/**/*", recursive=True)
        print(f"\n--- Upload Final para gs://{bucket_name}/{destination_blob_folder} ---")
        for file_path in files:
            if os.path.isfile(file_path):
                relative_path = os.path.relpath(file_path, source_folder)
                blob_path = os.path.join(destination_blob_folder, relative_path)
                try:
                    bucket.blob(blob_path).upload_from_filename(file_path)
                    print(f" [UPLOAD] {relative_path}")
                except Exception as e:
                    print(f" [ERRO] {relative_path}: {e}")
    except Exception as e:
        print(f"ERRO CRÍTICO NO UPLOAD: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline VivaReal")
    parser.add_argument("--only-links", action="store_true")
    parser.add_argument("--link", type=str)
    parser.add_argument("--paginas", type=int, default=5)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--url-base", type=str)
    parser.add_argument("--bucket", type=str)
    # Novo argumento para limitar links e testar rápido
    parser.add_argument("--limite-links", type=int, help="Limita a qtd de links processados")
    args = parser.parse_args()

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)

    # --- NOVO: Configura variável de ambiente Global ---
    if args.bucket:
        os.environ["GCS_BUCKET_NAME"] = args.bucket
        print(f"Configurado bucket global: {args.bucket}")

    try:
        if args.link:
            asyncio.run(extrair_dados_link_async(args.link, not args.no_headless, args.out_dir))
        elif args.only_links:
            asyncio.run(capturar_links_async(args.paginas, not args.no_headless, args.out_dir, args.url_base))
        else:
            run_full_pipeline(
                num_pages=args.paginas,
                links_limit=args.limite_links, # Passa o limite novo
                headless=not args.no_headless,
                out_dir=args.out_dir,
                base_url=args.url_base
            )
    except Exception as e:
        logger.error(f"ERRO FATAL: {e}")
    finally:
        if args.bucket:
            folder_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            upload_to_bucket(args.out_dir, args.bucket, f"execucao_{folder_date}")