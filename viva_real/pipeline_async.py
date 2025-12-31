import csv
import logging
import asyncio
from pathlib import Path
from typing import List

from viva_real.scraper_async import VivaRealScraper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def read_links_from_csv(path: Path) -> List[str]:
    links = []
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        field = reader.fieldnames[0] # Pega primeira coluna
        for row in reader:
            if row.get(field): links.append(row[field].strip())
    # Remove duplicados preservando ordem
    return list(dict.fromkeys(links))

async def run_pipeline_async(links_csv: str | Path, out_csv: str | None = None, headless: bool = True, limit: int | None = None) -> None:
    links_path = Path(links_csv)
    links = read_links_from_csv(links_path)
    
    if limit:
        links = links[:limit]
        logger.info(f"Limitando a {len(links)} links.")

    # AQUI ESTA A MUDANÇA:
    # Em vez de loop for aqui, passamos tudo para o scraper gerenciar a sessão
    scraper = VivaRealScraper(csv_path=out_csv, headless=headless)
    
    logger.info("Enviando lote de links para o scraper (Sessão Única)...")
    await scraper.scrape_batch(links)

def run_pipeline(links_csv: str | Path, out_csv: str | None = None, headless: bool = True, limit: int | None = None) -> None:
    asyncio.run(run_pipeline_async(links_csv, out_csv, headless, limit))