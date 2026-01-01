import csv
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, urljoin
from google.cloud import storage # Import necessário

logger = logging.getLogger(__name__)

class VivaRealLinkScraper:
    def __init__(self, base_url: str = None, output_dir: str = "output/links", headless: bool = True):
        self.base_url = base_url
        self.output_dir = output_dir
        self.headless = headless
        # Pega configurações de ambiente
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        self.execution_folder = os.environ.get("GCS_EXECUTION_FOLDER")
        
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _generate_output_path(self, total_links: int) -> str:
        # Usa o nome do bairro ou timestamp no arquivo
        prefix = "links"
        if "pinheiros" in self.base_url: prefix = "pinheiros"
        elif "itaim" in self.base_url: prefix = "itaim"
        elif "moema" in self.base_url: prefix = "moema"
        elif "jardins" in self.base_url: prefix = "jardins"
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(Path(self.output_dir) / f"{prefix}_{timestamp}_{total_links}.csv")
    
    def _upload_links(self, file_path: str):
        """Sobe o arquivo de links imediatamente para a pasta correta."""
        if not self.bucket_name or not self.execution_folder: return
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            # Estrutura: execucao_DATA/links/arquivo.csv
            blob_name = f"{self.execution_folder}/links/{os.path.basename(file_path)}"
            bucket.blob(blob_name).upload_from_filename(file_path)
            logger.info(f"📤 Links enviados para GS: {blob_name}")
        except Exception as e:
            logger.error(f"Erro upload links: {e}")

    async def _setup_browser(self, playwright) -> Browser:
        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-gpu"]
        )
        return browser

    async def _setup_context(self, browser: Browser) -> BrowserContext:
        return await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            java_script_enabled=True,
            locale="pt-BR",
        )

    async def _extract_links_from_page(self, page: Page) -> List[Dict[str, str]]:
        try:
            await page.wait_for_selector('li[data-cy="rp-property-cd"]', timeout=30000)
        except:
            return [] # Retorna vazio se der timeout na lista
            
        cards = page.locator('li[data-cy="rp-property-cd"]')
        count = await cards.count()
        logger.info(f"Encontrados {count} imóveis nesta página")

        results = []
        for i in range(count):
            card = cards.nth(i)
            link_el = card.locator("a[href]")
            if await link_el.count():
                link = await link_el.get_attribute("href")
                if link:
                    try:
                        full = urljoin(page.url, link)
                        results.append({"link_anuncio": full})
                    except: pass
        return results

    def _save_links_csv(self, links: List[Dict[str, str]], csv_path: str) -> None:
        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["link_anuncio"])
            writer.writeheader()
            writer.writerows(links)

    async def scrape_links(self, num_pages: int = 5) -> Optional[str]:
        results = []
        async with async_playwright() as p:
            browser = await self._setup_browser(p)
            context = await self._setup_context(browser)
            page = await context.new_page()
            
            try:
                for page_number in range(1, num_pages + 1):
                    parsed = urlparse(self.base_url)
                    qs = dict(parse_qsl(parsed.query))
                    qs["page"] = str(page_number)
                    new_query = urlencode(qs, doseq=True)
                    url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                    
                    logger.info(f"Página {page_number}: {url}")
                    await page.goto(url=url, wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(2) # Pausa leve
                    
                    page_results = await self._extract_links_from_page(page)
                    results.extend(page_results)

                # Remove duplicados e vazios
                seen = set()
                valid_results = []
                for r in results:
                    l = r.get("link_anuncio")
                    if l and l not in seen:
                        seen.add(l)
                        valid_results.append(r)
                
                if valid_results:
                    csv_path = self._generate_output_path(len(valid_results))
                    self._save_links_csv(valid_results, csv_path)
                    
                    # UPLOAD IMEDIATO
                    self._upload_links(csv_path)
                    
                    return csv_path
                return None

            except Exception as e:
                logger.error(f"Erro links: {e}")
                return None
            finally:
                await browser.close()