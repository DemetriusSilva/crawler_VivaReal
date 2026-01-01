import glob
import os
import logging
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from google.cloud import storage

from viva_real.scraper_async import VivaRealScraper
from viva_real.captura_links_async import VivaRealLinkScraper

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO MANUAL DAS URLs (A SOLUÇÃO DO PROBLEMA) ---
# Mapeamos o bairro diretamente para a URL funcional (com Zona e filtros corretos)
URLS_POR_BAIRRO = {
    "jardins": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-oeste/jardins/?transacao=venda",
    "pinheiros": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-oeste/pinheiros/?transacao=venda",
    "itaim-bibi": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-sul/itaim-bibi/?transacao=venda",
    "moema": "https://www.vivareal.com.br/venda/sp/sao-paulo/zona-sul/moema/?transacao=venda"
}

# Lista ordenada de execução
BAIRROS_ALVO = ["pinheiros", "itaim-bibi", "moema", "jardins"]

async def capturar_links_bairro(bairro: str, num_pages: int, headless: bool, out_dir: str) -> Optional[str]:
    """Captura links usando a URL específica do dicionário."""
    # Recupera a URL exata do dicionário. Se não achar, lança erro.
    url = URLS_POR_BAIRRO.get(bairro)
    if not url:
        logger.error(f"URL não configurada para o bairro: {bairro}")
        return None

    logger.info(f"--- Iniciando captura para: {bairro.upper()} ---")
    logger.info(f"URL Base: {url}")
    
    links_dir = str(Path(out_dir) / "links")
    link_scraper = VivaRealLinkScraper(base_url=url, output_dir=links_dir, headless=headless)
    
    # O scraper vai adicionar &page=N automaticamente na URL
    return await link_scraper.scrape_links(num_pages)

def upload_final_folder(source_folder, bucket_name, destination_folder):
    """Upload de varredura final."""
    try:
        if not bucket_name: return
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        files = glob.glob(f"{source_folder}/**/*", recursive=True)
        print(f"\n--- Upload Final de Sincronização ---")
        
        count = 0
        for file_path in files:
            if os.path.isfile(file_path):
                relative_path = os.path.relpath(file_path, source_folder)
                blob_path = f"{destination_folder}/{relative_path}".replace("\\", "/")
                try:
                    bucket.blob(blob_path).upload_from_filename(file_path)
                    count += 1
                except Exception as e:
                    print(f"Erro upload final {relative_path}: {e}")
        print(f"Sincronização concluída: {count} arquivos enviados.")
    except Exception as e:
        print(f"Erro crítico no upload final: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline VivaReal - Bairros SP")
    parser.add_argument("--paginas", type=int, default=5)
    parser.add_argument("--limite-links", type=int, help="Limita a qtd de links processados (TESTE)")
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--out-dir", default="output")
    parser.add_argument("--bucket", type=str)
    args = parser.parse_args()

    # 1. SETUP
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"execucao_{timestamp}"
    
    if args.bucket:
        os.environ["GCS_BUCKET_NAME"] = args.bucket
        os.environ["GCS_EXECUTION_FOLDER"] = folder_name
        logger.info(f"📂 Pasta no Storage definida: {folder_name}")

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    
    # Cria pasta de links manualmente para garantir que upload suba algo mesmo se vazio
    (Path(args.out_dir) / "links").mkdir(parents=True, exist_ok=True)

    all_links_files = []

    try:
        # 2. CAPTURA DE LINKS (Loop corrigido com URLs manuais)
        logger.info(f"Iniciando varredura nos bairros: {BAIRROS_ALVO}")
        
        for bairro in BAIRROS_ALVO:
            csv_path = asyncio.run(capturar_links_bairro(
                bairro=bairro,
                num_pages=args.paginas,
                headless=not args.no_headless,
                out_dir=args.out_dir
            ))
            if csv_path:
                all_links_files.append(csv_path)

        # 3. CONSOLIDAÇÃO
        total_links = []
        for fpath in all_links_files:
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()[1:] # Pula header
                    total_links.extend([l.strip() for l in lines if l.strip()])
            except: pass
        
        total_links = list(set(total_links))
        logger.info(f"Total de links únicos encontrados (todos os bairros): {len(total_links)}")

        if not total_links:
            logger.error("❌ Nenhum link encontrado. Verifique se as URLs dos bairros ainda são válidas.")
            # Não damos exit aqui para garantir que o 'finally' rode e suba logs se houver
        else:
            # 4. LIMITADOR (Opcional)
            if args.limite_links:
                logger.warning(f"⚠️ LIMITADOR ATIVO: Processando apenas {args.limite_links} links.")
                total_links = total_links[:args.limite_links]

            # 5. EXTRAÇÃO DE DADOS
            logger.info("Iniciando extração de dados...")
            dados_filename = f"{args.out_dir}/dados/{timestamp}_vivareal_consolidado.csv"
            
            scraper = VivaRealScraper(csv_path=dados_filename, headless=not args.no_headless)
            asyncio.run(scraper.scrape_batch(total_links))

    except Exception as e:
        logger.error(f"ERRO FATAL: {e}")
    
    finally:
        # 6. UPLOAD FINAL (Garante que logs e pastas vazias sejam processados)
        if args.bucket:
            upload_final_folder(args.out_dir, args.bucket, folder_name)