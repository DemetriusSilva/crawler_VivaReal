import csv
import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from viva_real.utils.functions_utils import parse_endereco
import json
from playwright.async_api import async_playwright, Browser, Page, BrowserContext

logger = logging.getLogger(__name__)

class VivaRealScraper:
    def __init__(self, csv_path: Optional[str] = None, headless: bool = True):
        """Inicializa o scraper e define o CSV de saída.
        
        Args:
            csv_path: Caminho para o arquivo CSV de saída. Se None, usa um nome padrão.
            headless: Se True, executa o navegador em modo headless.
        """
        if csv_path is None:
            data_capt = datetime.now().strftime("%Y%m%d")
            self.csv_path = f"output/dados/{data_capt}_vivareal.csv"
        else:
            self.csv_path = csv_path
            
        self.headless = headless

        # Define as colunas do CSV
        # CSV fields - inclui componentes de endereço mapeados e características separadas
        self.fields = [
            "nome_anunciante", "tipo_transacao", "preco_venda", "endereco",
            "logradouro", "numero", "bairro", "municipio", "uf",
            "metragem", "quartos", "banheiros", "suites", "vagas", "outros", "caracteristicas",
            "latitude", "longitude", "condominio", "iptu", "qtd_imagens", "urls_imagens", "data_extracao", "link"
        ]
        
        self._ensure_output_dir()

        # Se o CSV não existe, cria com cabeçalho
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields)
                writer.writeheader()
    
    def _ensure_output_dir(self) -> None:
        """Garante que o diretório de saída existe."""
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
    
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

    async def _safe_text(self, page: Page, selector: str) -> Optional[str]:
        """Extrai texto de um elemento de forma segura."""
        el = page.locator(selector)
        if await el.count() > 0:
            return await el.inner_text()
        return None

    async def extrair_caracteristicas(self, page: Page) -> Dict[str, Any]:
        """Extrai características do imóvel e organiza em colunas específicas.

        Retorna um dict com chaves: metragem, quartos, banheiros, suites, vagas, outros (lista), caracteristicas (lista completa).
        """
        elementos = page.locator(".amenities-item-text")
        count = await elementos.count()
        caracteristicas: List[str] = []
        for i in range(count):
            texto = await elementos.nth(i).inner_text()
            if texto:
                caracteristicas.append(texto.strip())

        lower = [c.lower() for c in caracteristicas]

        def _find(pred):
            for idx, c in enumerate(lower):
                if pred(c):
                    return caracteristicas[idx]
            return None

        metragem = _find(lambda x: "m²" in x or "m2" in x)
        quartos = _find(lambda x: "quarto" in x)
        banheiros = _find(lambda x: "banheiro" in x)
        suites = _find(lambda x: "suíte" in x or "suite" in x or "suítes" in x)
        vagas = _find(lambda x: "vaga" in x)

        classificados = {metragem, quartos, banheiros, suites, vagas}
        outros = [c for c in caracteristicas if c not in classificados]

        return {
            "metragem": metragem,
            "quartos": quartos,
            "banheiros": banheiros,
            "suites": suites,
            "vagas": vagas,
            "outros": outros,
            "caracteristicas": caracteristicas,
        }

    def _extract_coordinates(self, iframe_src: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        """Extrai coordenadas do src do iframe do mapa."""
        if iframe_src and "q=" in iframe_src:
            coords = iframe_src.split("q=")[-1].split("&")[0]
            return coords.split(",")
        return None, None

    async def _extract_data(self, page: Page, link: str) -> Dict[str, Any]:
        """Extrai todos os dados de uma página de anúncio."""
        # Extrai características detalhadas (metragem, quartos, banheiros, suites, vagas, outros)
        parsed_features = await self.extrair_caracteristicas(page)
        caracteristicas_lista = parsed_features.get("caracteristicas", [])

        iframe = page.locator('iframe[data-testid="map-iframe"]')
        iframe_src = await iframe.get_attribute("src") if await iframe.count() > 0 else None
        latitude, longitude = self._extract_coordinates(iframe_src)

        nome_el = page.locator('a[data-testid="official-store-redirect-link"]').first
        nome_anunciante = await nome_el.inner_text() if await nome_el.count() > 0 else None

        # Extração das imagens (src, data-src, srcset)
        img_selectors = [
            ".olx-core-carousel__container img",
            "ul.carousel-photos--wrapper img",
            "img[property='image']",
            "img[src*='resizedimgs.vivareal']"
        ]
        img_elements = []
        for sel in img_selectors:
            img_elements += await page.locator(sel).all()
        img_urls = []
        for img in img_elements:
            src = await img.get_attribute("src")
            data_src = await img.get_attribute("data-src")
            srcset = await img.get_attribute("srcset")
            url = src or data_src or srcset
            if not url:
                continue
            # se srcset, pega a primeira url
            if srcset:
                url = srcset.split(",")[0].strip().split()[0]
            # filtra urls inválidas ou placeholders
            if "{description}" in url or url.endswith("{description}.webp"):
                continue
            img_urls.append(url)
        # Remove duplicados
        img_urls = list(dict.fromkeys(img_urls))

        endereco_text = await self._safe_text(page, 'p.location-address__text[data-testid="location-address"]')
        # parse address components (heuristic)
        parsed = parse_endereco(endereco_text) if endereco_text else {"logradouro": None, "numero": None, "bairro": None, "municipio": None, "uf": None}

        return {
            "nome_anunciante": nome_anunciante.strip() if nome_anunciante else None,
            "tipo_transacao": await self._safe_text(page, "div.price-info__values-sale div.value-item:nth-of-type(1) .value-item__title, div.price-info__values-both div.value-item:nth-of-type(1) .value-item__title"),
            "preco_venda": await self._safe_text(page, "div.value-item:nth-of-type(1) > p.value-item__value"),
            "endereco": endereco_text,
            "logradouro": parsed.get("logradouro"),
            "numero": parsed.get("numero"),
            "bairro": parsed.get("bairro"),
            "municipio": parsed.get("municipio"),
            "uf": parsed.get("uf"),
            "metragem": parsed_features.get("metragem"),
            "quartos": parsed_features.get("quartos"),
            "banheiros": parsed_features.get("banheiros"),
            "suites": parsed_features.get("suites"),
            "vagas": parsed_features.get("vagas"),
            "outros": json.dumps(parsed_features.get("outros", []), ensure_ascii=False),
            "caracteristicas": caracteristicas_lista,
            "latitude": latitude,
            "longitude": longitude,
            "condominio": await self._safe_text(page, '[data-testid="condoFee"]'),
            "iptu": await self._safe_text(page, '[data-testid="iptu"]'),
            "qtd_imagens": len(img_urls),
            "urls_imagens": "; ".join(img_urls),
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "link": link,
        }

    def _save_to_csv(self, data: Dict[str, Any]) -> None:
        """Salva os dados extraídos no arquivo CSV, garantindo que urls_imagens fique entre aspas duplas."""
        # Garante que urls_imagens seja string entre aspas duplas
        if "urls_imagens" in data and isinstance(data["urls_imagens"], str):
            data["urls_imagens"] = f'"{data["urls_imagens"]}"'
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writerow(data)

    async def scrape_link(self, link: str, timeout_ms: int = 60000, retries: int = 2, save_debug: bool = True) -> bool:
        """Raspa os dados de um único anúncio e salva no CSV.
        
        Args:
            link: URL do anúncio para extrair dados.
            
        Returns:
            bool: True se sucesso, False se falha.
        """
        async with async_playwright() as p:
            browser = await self._setup_browser(p)
            context = await self._setup_context(browser)
            page = await context.new_page()

            # Selectors fallback: some listings use different templates
            selectors = [
                "div.details",
                "main",
                "div[data-testid='ad-detail']",
                "section[data-testid='listing-details']",
            ]

            attempt = 0
            while attempt <= retries:
                attempt += 1
                try:
                    await page.goto(url=link, timeout=timeout_ms)

                    # wait for any of the possible selectors
                    joined = ", ".join(selectors)
                    await page.wait_for_selector(joined, timeout=timeout_ms)

                    data = await self._extract_data(page, link)
                    self._save_to_csv(data)

                    logger.info(f"✅ Dados salvos para: {link}")
                    await browser.close()
                    return True

                except Exception as e:
                    # timeout or other issue
                    logger.warning(f"Tentativa {attempt}/{retries+1} falhou para {link}: {e}")

                    if save_debug:
                        # salva debug artifacts para investigação
                        debug_dir = os.path.join("output", "debug")
                        os.makedirs(debug_dir, exist_ok=True)
                        safe_name = (link.replace("https://", "").replace("/", "_").replace("?", "_") )[:180]
                        screenshot_path = os.path.join(debug_dir, f"{safe_name}_attempt{attempt}.png")
                        html_path = os.path.join(debug_dir, f"{safe_name}_attempt{attempt}.html")
                        try:
                            await page.screenshot(path=screenshot_path, full_page=True)
                            content = await page.content()
                            with open(html_path, "w", encoding="utf-8") as fh:
                                fh.write(content)
                            logger.info(f"Debug salvo: {screenshot_path}, {html_path}")
                        except Exception as dbg_e:
                            logger.debug(f"Falha ao salvar debug artifacts: {dbg_e}")

                    # se não restarem tentativas, loga erro e fecha
                    if attempt > retries:
                        logger.error(f"❌ Erro ao raspar {link}: {e}")
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        return False

                    # backoff breve antes de tentar novamente
                    await asyncio.sleep(2 ** attempt)

def run_scraper(link: str, csv_path: Optional[str] = None) -> None:
    """Função auxiliar para executar o scraper em modo síncrono."""
    scraper = VivaRealScraper(csv_path)
    asyncio.run(scraper.scrape_link(link))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    link = "https://www.vivareal.com.br/imoveis-lancamento/jardim-lobato-2778226554/?source=ranking%2Crp"
    run_scraper(link)