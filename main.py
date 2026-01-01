import glob
import os
import logging
import asyncio
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from google.cloud import storage

from viva_real.scraper_async import VivaRealScraper
from viva_real.captura_links_async import VivaRealLinkScraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DAS URLS COMPLETAS ---
# Essas URLs contêm o "contexto geográfico" (parametro 'onde') necessário para os filtros funcionarem.
URLS_BASE_BAIRRO = {
    "moema": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-sul/moema/?transacao=venda&onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Paulo%2CZona+Sul%2CMoema%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Paulo%3EZona+Sul%3EMoema%2C-23.612476%2C-46.661547%2C",
    
    "jardins": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-oeste/jardins/?transacao=venda&onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Paulo%2CZona+Oeste%2CJardins%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Paulo%3EZona+Oeste%3EJardins%2C-23.573979%2C-46.660691%2C",
    
    "pinheiros": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-oeste/pinheiros/?transacao=venda&onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Paulo%2CZona+Oeste%2CPinheiros%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Paulo%3EZona+Oeste%3EPinheiros%2C-23.563579%2C-46.691607%2C",
    
    "itaim-bibi": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-sul/itaim-bibi/?transacao=venda&onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Paulo%2CZona+Sul%2CItaim+Bibi%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Paulo%3EZona+Sul%3EItaim+Bibi%2C-23.583748%2C-46.678074%2C"
}

# --- ESTRATÉGIAS (Sufixos de Ordenação da API Nova) ---
STRATEGIES = {
    "padrao": "&ordem=MOST_RELEVANT",
    "recentes": "&ordem=MOST_RECENT",
    "menor_preco": "&ordem=LOWEST_PRICE",
    "maior_preco": "&ordem=HIGHEST_PRICE",
    "maior_area": "&ordem=BIGGEST_AREA",
    "menor_area": "&ordem=SMALLEST_AREA"
}

BAIRROS_ALVO = ["pinheiros", "itaim-bibi", "moema", "jardins"]

def limpar_url_base(url: str) -> str:
    """Remove ordenação existente para não conflitar com a estratégia escolhida."""
    # Remove qualquer variação de &ordem=... ou ?ordem=...
    clean_url = re.sub(r'[&?]ordem=[^&]+', '', url)
    
    # Garante que a URL termine pronta para receber novos parametros
    if '?' not in clean_url:
        clean_url += '?'
    elif not clean_url.endswith('&'):
        clean_url += '&'
    return clean_url

async def capturar_links_bairro(bairro: str, num_pages: int, headless: bool, out_dir: str, strategy_suffix: str) -> Optional[str]:
    raw_url = URLS_BASE_BAIRRO.get(bairro)
    if not raw_url: return None

    # 1. Limpa a URL base (remove o MOST_RELEVANT que veio no copy-paste)
    base_clean = limpar_url_base(raw_url)
    
    # 2. Aplica a estratégia do dia
    # Remove o '&' inicial do sufixo se a base já tiver '&' para evitar duplicidade
    suffix_clean = strategy_suffix.lstrip('&')
    final_url = f"{base_clean}{suffix_clean}"
    
    logger.info(f"--- {bairro.upper()} ---")
    logger.info(f"🔗 URL Final: {final_url}")
    
    links_dir = str(Path(out_dir) / "links")
    link_scraper = VivaRealLinkScraper(base_url=final_url, output_dir=links_dir, headless=headless)
    return await link_scraper.scrape_links(num_pages)

def upload_final_folder(source_folder, bucket_name, destination_folder):
    """Sobe logs e arquivos residuais no final da execução."""
    try:
        if not bucket_name: return
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        files = glob.glob(f"{source_folder}/**/*", recursive=True)
        print(f"\n--- Upload Final de Sincronização ---")
        for file_path in files:
            if os.path.isfile(file_path):
                relative_path = os.path.relpath(file_path, source_folder)
                blob_path = f"{destination_folder}/{relative_path}".replace("\\", "/")
                try:
                    bucket.blob(blob_path).upload_from_filename(file_path)
                except: pass
    except: pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--paginas", type=int, default=5)
    parser.add_argument("--limite-links", type=int)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--strategy", type=str, default="padrao", choices=STRATEGIES.keys())
    args = parser.parse_args()

    strategy_suffix = STRATEGIES[args.strategy]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"execucao_{args.strategy}_{timestamp}"
    
    if args.bucket:
        os.environ["GCS_BUCKET_NAME"] = args.bucket
        os.environ["GCS_EXECUTION_FOLDER"] = folder_name

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    (Path(args.out_dir) / "links").mkdir(parents=True, exist_ok=True)

    try:
        all_links_files = []
        logger.info(f"🚀 INICIANDO VARREDURA: {args.strategy.upper()}")
        logger.info(f"📁 Pasta Destino: gs://{args.bucket}/{folder_name}")
        
        # 1. CAPTURA DE LINKS (Itera Bairros)
        for bairro in BAIRROS_ALVO:
            csv_path = asyncio.run(capturar_links_bairro(
                bairro=bairro,
                num_pages=args.paginas,
                headless=not args.no_headless,
                out_dir=args.out_dir,
                strategy_suffix=strategy_suffix
            ))
            if csv_path: all_links_files.append(csv_path)

        # 2. CONSOLIDAÇÃO DE LINKS
        total_links = []
        for fpath in all_links_files:
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()[1:] # Pula header
                    total_links.extend([l.strip() for l in lines if l.strip()])
            except: pass
        
        # Remove duplicatas
        total_links = list(set(total_links))
        logger.info(f"Total links únicos ({args.strategy}): {len(total_links)}")

        if total_links:
            # Aplica limite se for teste
            if args.limite_links: 
                logger.warning(f"⚠️ Limitando a {args.limite_links} links.")
                total_links = total_links[:args.limite_links]
            
            # 3. EXTRAÇÃO DE DADOS
            dados_filename = f"{args.out_dir}/dados/{timestamp}_vivareal_{args.strategy}.csv"
            scraper = VivaRealScraper(csv_path=dados_filename, headless=not args.no_headless)
            asyncio.run(scraper.scrape_batch(total_links))
        else:
            logger.error("❌ Nenhum link capturado. Verifique as URLs.")

    except Exception as e:
        logger.error(f"ERRO CRÍTICO: {e}")
    finally:
        if args.bucket:
            upload_final_folder(args.out_dir, args.bucket, folder_name)