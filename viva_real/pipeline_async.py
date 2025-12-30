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
        raise FileNotFoundError(f"Arquivo de links não encontrado: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Tenta encontrar o campo mais comum
        possible_fields = ["link_anuncio", "link", "url"]
        field = None
        for pf in possible_fields:
            if pf in reader.fieldnames:
                field = pf
                break
        if not field:
            # fallback para a primeira coluna
            field = reader.fieldnames[0]

        for row in reader:
            val = row.get(field)
            if val:
                links.append(val.strip())

    # remove duplicatas mantendo ordem
    seen = set()
    unique = []
    for l in links:
        if l not in seen:
            seen.add(l)
            unique.append(l)
    return unique


async def run_pipeline_async(links_csv: str | Path, out_csv: str | None = None, headless: bool = True, limit: int | None = None) -> None:
    links_path = Path(links_csv)
    links = read_links_from_csv(links_path)
    total = len(links)
    logger.info("Links lidos: %d (arquivo=%s)", total, links_path)

    if limit:
        links = links[:limit]
        logger.info("Limitando a %d links para esta execução", len(links))

    # Usa o mesmo CSV de saída configurado ou o fornecido
    scraper = VivaRealScraper(csv_path=out_csv, headless=headless)
    processed = 0
    skipped = 0
    for i, link in enumerate(links, start=1):
        logger.info("[%d/%d] Processando: %s", i, total, link)
        try:
            if await scraper.scrape_link(link):
                processed += 1
            else:
                skipped += 1
        except Exception as e:
            logger.warning("Erro ao processar %s: %s", link, e)
            skipped += 1

    logger.info("Pipeline concluída. Processados: %d, Pulados: %d, Total fornecido: %d", processed, skipped, total)


def run_pipeline(links_csv: str | Path, out_csv: str | None = None, headless: bool = True, limit: int | None = None) -> None:
    """Wrapper síncrono para executar o pipeline."""
    asyncio.run(run_pipeline_async(links_csv, out_csv, headless, limit))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline: ler CSV de links e extrair detalhes em CSV unificado")
    parser.add_argument("links_csv", help="Caminho para o CSV contendo os links (coluna: link_anuncio / link / url)")
    parser.add_argument("--out-csv", help="CSV de saída para os detalhes (opcional)")
    parser.add_argument("--no-headless", action="store_true", help="Executar com UI do navegador para debug")
    parser.add_argument("--limit", type=int, help="Limitar número de links a processar (útil para testes)")
    args = parser.parse_args()

    run_pipeline(args.links_csv, out_csv=args.out_csv, headless=not args.no_headless, limit=args.limit)