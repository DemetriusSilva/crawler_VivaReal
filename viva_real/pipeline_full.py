import logging
import asyncio
from pathlib import Path
from typing import Optional

from viva_real.captura_links_async import VivaRealLinkScraper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


async def run_full_pipeline_async(
    num_pages: int = 5,
    links_limit: Optional[int] = None,
    headless: bool = True,
    out_dir: Optional[str] = None,
    base_url: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """Executa o pipeline completo de captura e processamento de links.
    
    Args:
        num_pages: Número de páginas de busca para capturar links
        links_limit: Limite de links para processar (None = todos)
        headless: Se True, executa navegador em modo headless
        out_dir: Diretório base para salvar arquivos de saída
        
    Returns:
        tuple[str | None, str | None]: Caminhos dos arquivos CSV (links, dados) ou None se falhar
    """
    # Configura diretórios de saída
    out_dir = out_dir or "output"
    links_dir = str(Path(out_dir) / "links")
    dados_dir = str(Path(out_dir) / "dados")

    try:
        # 1. Captura links
        logger.info(f"Iniciando captura de links de {num_pages} páginas...")
        link_scraper = VivaRealLinkScraper(base_url=base_url, output_dir=links_dir, headless=headless)
        links_csv = await link_scraper.scrape_links(num_pages)
        
        if not links_csv:
            logger.error("Falha ao capturar links. Pipeline interrompido.")
            return None, None
        # 2. Processa links capturados
        logger.info(f"Iniciando processamento dos links de {links_csv}...")

        # garante que o diretório de dados exista
        Path(dados_dir).mkdir(parents=True, exist_ok=True)

        # Configura nome do arquivo de saída
        timestamp = Path(links_csv).stem.split("_")[1]  # Extrai timestamp do nome do arquivo
        dados_csv = str(Path(dados_dir) / f"{timestamp}_vivareal.csv")

        # Executa pipeline de processamento (assíncrono)
        from viva_real.pipeline_async import run_pipeline_async
        await run_pipeline_async(links_csv, out_csv=dados_csv, headless=headless, limit=links_limit)

        return links_csv, dados_csv

    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        return None, None


def run_full_pipeline(
    num_pages: int = 5,
    links_limit: Optional[int] = None,
    headless: bool = True,
    out_dir: Optional[str] = None,
    base_url: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """Wrapper síncrono para executar o pipeline completo."""
    return asyncio.run(run_full_pipeline_async(num_pages, links_limit, headless, out_dir, base_url))


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline completo: captura links e extrai dados do VivaReal")
    parser.add_argument("--paginas", type=int, default=5, help="Número de páginas para capturar links (padrão: 5)")
    parser.add_argument("--limite-links", type=int, help="Limitar número de links a processar")
    parser.add_argument("--no-headless", action="store_true", help="Executar com UI do navegador para debug")
    parser.add_argument("--out-dir", help="Diretório base para arquivos de saída (padrão: output)")
    args = parser.parse_args()
    
    links_csv, dados_csv = run_full_pipeline(
        num_pages=args.paginas,
        links_limit=args.limite_links,
        headless=not args.no_headless,
        out_dir=args.out_dir
    )
    
    if links_csv and dados_csv:
        print(f"\nPipeline concluído com sucesso!")
        print(f"Links salvos em: {links_csv}")
        print(f"Dados salvos em: {dados_csv}")
    else:
        print("\nFalha na execução do pipeline")