import csv
import os
import logging
import asyncio
import random
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, urljoin
# ATUALIZAÇÃO: Importando a classe da nova versão 2.0+
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

class VivaRealLinkScraper:
    def __init__(self, base_url: str = None, output_dir: str = "output/links", headless: bool = True):
        self.base_url = base_url or "https://www.vivareal.com.br/venda/sp/sao-paulo/?tipos=apartamento_residencial&ordem=MOST_RECENT"
        self.output_dir = output_dir
        self.headless = headless
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _generate_output_path(self, total_links: int, prefix: str = "apartamento") -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(Path(self.output_dir) / f"{prefix}_{timestamp}_{total_links}_links.csv")
    
    async def _setup_browser(self, playwright) -> Browser:
        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--window-size=1920,1080",
                "--ignore-certificate-errors",
                "--disable-extensions",
                "--disable-infobars"
            ]
        )
        return browser

    async def _setup_context(self, browser: Browser) -> BrowserContext:
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            permissions=["geolocation"],
            geolocation={"latitude": -23.5505, "longitude": -46.6333}, # SP
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Sec-Ch-Ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
                "Dnt": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1"
            }
        )
        return context

    async def _human_behavior(self, page: Page):
        """Simula comportamento humano na página."""
        try:
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 1000)
                y = random.randint(100, 800)
                await page.mouse.move(x, y, steps=10)
                await asyncio.sleep(random.uniform(0.1, 0.5))
            
            await page.mouse.wheel(0, random.randint(100, 500))
            await asyncio.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

    async def _extract_links_from_page(self, page: Page) -> List[Dict[str, str]]:
        await page.wait_for_selector('li[data-cy="rp-property-cd"]', timeout=90000)
        
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
                    except Exception:
                        full = link
                    results.append({"link_anuncio": full})
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
            
            # ATUALIZAÇÃO: Aplica o Stealth no CONTEXTO (Nova API v2.0+)
            # Isso deve ser feito antes de criar a página
            stealth = Stealth()
            await stealth.apply_stealth_async(context)
            
            page = await context.new_page()
            
            try:
                # Aquecimento na home
                logger.info("Acessando home para aquecimento...")
                await page.goto("https://www.vivareal.com.br/", wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(2, 4))

                for page_number in range(1, num_pages + 1):
                    parsed = urlparse(self.base_url)
                    qs = dict(parse_qsl(parsed.query))
                    qs["page"] = str(page_number)
                    new_query = urlencode(qs, doseq=True)
                    url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                    
                    logger.info(f"Navegando para a página {page_number}...")

                    await page.goto(url=url, wait_until="domcontentloaded", timeout=90000)
                    
                    await self._human_behavior(page)
                    wait_time = random.uniform(5, 10)
                    logger.info(f"Aguardando {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                    
                    page_results = await self._extract_links_from_page(page)
                    results.extend(page_results)

                valid_results = [r for r in results if r.get("link_anuncio")]
                total_links = len(valid_results)
                
                if total_links > 0:
                    csv_path = self._generate_output_path(total_links)
                    self._save_links_csv(valid_results, csv_path)
                    logger.info(f"✅ {total_links} links salvos em {csv_path}")
                    return csv_path
                else:
                    logger.warning("❌ Nenhum link válido encontrado")
                    return None

            except Exception as e:
                logger.error(f"❌ Erro ao capturar links: {e}")
                
                try:
                    debug_dir = Path(self.output_dir).parent / "debug"
                    debug_dir.mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    screenshot_path = debug_dir / f"erro_captura_{timestamp}.png"
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                    logger.info(f"📸 Screenshot salvo: {screenshot_path}")
                    
                    html_path = debug_dir / f"erro_captura_{timestamp}.html"
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                        
                except Exception as screenshot_error:
                    logger.error(f"Falha ao salvar debug: {screenshot_error}")
                
                return None
            finally:
                await browser.close()

def run_link_scraper(num_pages: int = 5, headless: bool = True) -> Optional[str]:
    scraper = VivaRealLinkScraper(headless=headless)
    return asyncio.run(scraper.scrape_links(num_pages))

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Captura links de anúncios do VivaReal")
    parser.add_argument("--paginas", type=int, default=5, help="Número de páginas para capturar")
    parser.add_argument("--no-headless", action="store_true", help="Executar com UI")
    args = parser.parse_args()
    csv_path = run_link_scraper(num_pages=args.paginas, headless=not args.no_headless)