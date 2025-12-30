import logging
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
from viva_real.pipeline_full import run_full_pipeline
from viva_real.captura_links_async import VivaRealLinkScraper
from viva_real.scraper_async import VivaRealScraper

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def capturar_links_async(num_pages: int = 5, headless: bool = True, out_dir: str = "output", base_url: Optional[str] = None) -> str:
    """Executa apenas a captura de links do VivaReal."""
    links_dir = str(Path(out_dir) / "links")
    link_scraper = VivaRealLinkScraper(base_url=base_url, output_dir=links_dir, headless=headless)
    return await link_scraper.scrape_links(num_pages)

async def extrair_dados_link_async(link: str, headless: bool = True, out_dir: str = "output") -> bool:
    """Extrai dados de um único link do VivaReal."""
    dados_dir = str(Path(out_dir) / "dados")
    timestamp = datetime.now().strftime("%Y%m%d")
    csv_path = str(Path(dados_dir) / f"{timestamp}_vivareal_single.csv")
    
    scraper = VivaRealScraper(csv_path=csv_path, headless=headless)
    return await scraper.scrape_link(link)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline de captura de dados do VivaReal")
    parser.add_argument("--only-links", action="store_true", help="Executar apenas a captura de links")
    parser.add_argument("--link", type=str, help="URL do imóvel para extrair dados")
    parser.add_argument("--paginas", type=int, default=5, help="Número de páginas para capturar links (padrão: 5)")
    parser.add_argument("--no-headless", action="store_true", help="Executar com UI do navegador para debug")
    parser.add_argument("--out-dir", default="output", help="Diretório base para arquivos de saída")
    parser.add_argument("--url-base", type=str, help="URL base para busca (ex: https://www.vivareal.com.br/venda/sp/sao-paulo/apartamento_residencial/)")
    args = parser.parse_args()
    
    if args.link:
        # Extrair dados de um único link
        success = asyncio.run(extrair_dados_link_async(
            link=args.link,
            headless=not args.no_headless,
            out_dir=args.out_dir
        ))
        if success:
            print(f"\nExtração de dados do link concluída com sucesso!")
            print(f"Dados salvos em: {args.out_dir}/dados/")
        else:
            print("\nFalha na extração de dados do link")
            
    elif args.only_links:
        # Executar apenas captura de links
        links_csv = asyncio.run(capturar_links_async(
            num_pages=args.paginas,
            headless=not args.no_headless,
            out_dir=args.out_dir,
            base_url=args.url_base
        ))
        if links_csv:
            print(f"\nCaptura de links concluída com sucesso!")
            print(f"Links salvos em: {links_csv}")
        else:
            print("\nFalha na captura de links")
    else:
        # Executar pipeline completo
        links_csv, dados_csv = run_full_pipeline(
            num_pages=args.paginas,
            links_limit=None,
            headless=not args.no_headless,
            out_dir=args.out_dir,
            base_url=args.url_base
        )
        
        if links_csv and dados_csv:
            print(f"\nPipeline concluído com sucesso!")
            print(f"Links salvos em: {links_csv}")
            print(f"Dados salvos em: {dados_csv}")
        else:
            print("\nFalha na execução do pipeline")