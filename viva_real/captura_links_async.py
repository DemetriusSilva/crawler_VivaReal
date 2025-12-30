import csv
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, urljoin

logger = logging.getLogger(__name__)

class VivaRealLinkScraper:
    def __init__(self, base_url: str = None, output_dir: str = "output/links", headless: bool = True):
        """Inicializa o capturador de links.
        
        Args:
            base_url: URL base para busca. Se None, usa a busca de apartamentos em SP.
            output_dir: Diretório para salvar os arquivos CSV de links.
            headless: Se True, executa o navegador em modo headless.
        """
        self.base_url = base_url or "https://www.vivareal.com.br/venda/sp/sao-paulo/?tipos=apartamento_residencial&ordem=MOST_RECENT"
        self.output_dir = output_dir
        self.headless = headless
        
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        """Garante que o diretório de saída existe."""
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _generate_output_path(self, total_links: int, prefix: str = "apartamento") -> str:
        """Gera o caminho do arquivo CSV de saída."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(Path(self.output_dir) / f"{prefix}_{timestamp}_{total_links}_links.csv")
    
    async def _setup_browser(self, playwright) -> Browser:
        """Configura e retorna uma instância do navegador."""
        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage"
            ]
        )
        return browser

    async def _setup_context(self, browser: Browser) -> BrowserContext:
        """Configura e retorna um novo contexto de navegação."""
        return await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            java_script_enabled=True,
            locale="pt-BR",
        )

    async def _extract_links_from_page(self, page: Page) -> List[Dict[str, str]]:
        """Extrai links de anúncios de uma página."""
        await page.wait_for_selector('li[data-cy="rp-property-cd"]')
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
                    # resolve links relativos para absolutos usando a URL atual da página
                    try:
                        full = urljoin(page.url, link)
                    except Exception:
                        full = link
                    results.append({"link_anuncio": full})
        
        return results

    def _save_links_csv(self, links: List[Dict[str, str]], csv_path: str) -> None:
        """Salva os links extraídos em um arquivo CSV."""
        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["link_anuncio"])
            writer.writeheader()
            writer.writerows(links)

    async def scrape_links(self, num_pages: int = 5) -> Optional[str]:
        """Captura links de anúncios de múltiplas páginas.
        
        Args:
            num_pages: Número de páginas para capturar links.
            
        Returns:
            str | None: Caminho do arquivo CSV com os links ou None se falhar.
        """
        results = []
        
        async with async_playwright() as p:
            browser = await self._setup_browser(p)
            context = await self._setup_context(browser)
            page = await context.new_page()
            
            try:
                for page_number in range(1, num_pages + 1):
                    # Constrói a URL corretamente adicionando/atualizando o parâmetro `page` na query string
                    parsed = urlparse(self.base_url)
                    qs = dict(parse_qsl(parsed.query))
                    qs["page"] = str(page_number)
                    new_query = urlencode(qs, doseq=True)
                    url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                    logger.info(f"Navegando para a página {page_number}: {url}")

                    await page.goto(url=url, wait_until="networkidle")
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
                return None
            finally:
                await browser.close()
                logger.info("Navegador fechado.")


def run_link_scraper(num_pages: int = 5, headless: bool = True) -> Optional[str]:
    """Função auxiliar para executar o scraper de links em modo síncrono."""
    scraper = VivaRealLinkScraper(headless=headless)
    return asyncio.run(scraper.scrape_links(num_pages))


if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    parser = argparse.ArgumentParser(description="Captura links de anúncios do VivaReal")
    parser.add_argument("--paginas", type=int, default=5, help="Número de páginas para capturar (padrão: 5)")
    parser.add_argument("--no-headless", action="store_true", help="Executar com UI do navegador para debug")
    args = parser.parse_args()
    
    csv_path = run_link_scraper(num_pages=args.paginas, headless=not args.no_headless)
    if csv_path:
        print(f"\nLinks salvos em: {csv_path}")
    else:
        print("\nFalha ao capturar links")