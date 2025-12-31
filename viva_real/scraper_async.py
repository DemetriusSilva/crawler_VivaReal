import csv
import os
import logging
import asyncio
import random
from datetime import datetime
from typing import Dict, List, Optional, Any
from viva_real.utils.functions_utils import parse_endereco
import json
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from playwright_stealth import Stealth
from google.cloud import storage

logger = logging.getLogger(__name__)

class VivaRealScraper:
    def __init__(self, csv_path: Optional[str] = None, headless: bool = False):
        if csv_path is None:
            data_capt = datetime.now().strftime("%Y%m%d")
            self.csv_path = f"output/dados/{data_capt}_vivareal.csv"
        else:
            self.csv_path = csv_path
        self.headless = headless
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")

        self.fields = [
            "nome_anunciante", "tipo_transacao", "preco_venda", "endereco",
            "logradouro", "numero", "bairro", "municipio", "uf",
            "metragem", "quartos", "banheiros", "suites", "vagas", "outros", "caracteristicas",
            "latitude", "longitude", "condominio", "iptu", "qtd_imagens", "urls_imagens", "data_extracao", "link"
        ]
        self._ensure_output_dir()
        
        # MUDANÇA 1: encoding="utf-8-sig" para o Excel ler acentos corretamente
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields)
                writer.writeheader()
    
    def _ensure_output_dir(self) -> None:
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
    
    def _upload_live_debug(self, file_path: str):
        if not self.bucket_name or not os.path.exists(file_path): return
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            blob_name = f"debug_live/{os.path.basename(file_path)}"
            bucket.blob(blob_name).upload_from_filename(file_path)
        except: pass

    async def _setup_browser(self, playwright) -> Browser:
        return await playwright.chromium.launch(
            headless=self.headless, 
            args=[
                "--disable-blink-features=AutomationControlled", 
                "--no-sandbox", 
                "--disable-gpu", 
                "--disable-dev-shm-usage", 
                "--window-size=1920,1080", 
                "--start-maximized",
                "--ignore-certificate-errors"
            ]
        )

    async def _setup_context(self, browser: Browser) -> BrowserContext:
        return await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin", "Sec-Fetch-User": "?1"
            }
        )

    async def _human_behavior(self, page: Page):
        try:
            try:
                await page.locator("button:has-text('Aceitar'), button:has-text('Prosseguir')").click(timeout=2000)
            except: pass
            await page.mouse.move(random.randint(100, 500), random.randint(100, 500), steps=20)
            await asyncio.sleep(0.5)
            await page.mouse.wheel(0, 300)
        except: pass

    async def _safe_text(self, page, selector):
        el = page.locator(selector).first
        # MUDANÇA 2: .strip() aqui ajuda a limpar espaços em branco extras (ex: quebras de linha no preço)
        text = await el.inner_text() if await el.count() > 0 else None
        return text.strip() if text else None

    async def extrair_caracteristicas(self, page):
        els = await page.locator(".amenities-item-text, [data-testid='amenities-item']").all_inner_texts()
        lower = [c.lower() for c in els]
        def _f(p): 
            for i,c in enumerate(lower): 
                if p(c): return els[i]
            return None
        return {
            "metragem": _f(lambda x: "m²" in x or "m2" in x),
            "quartos": _f(lambda x: "quarto" in x),
            "banheiros": _f(lambda x: "banheiro" in x),
            "suites": _f(lambda x: "suíte" in x or "suite" in x),
            "vagas": _f(lambda x: "vaga" in x),
            "outros": [c for c in els if "m²" not in c.lower() and "quarto" not in c.lower() and "banheiro" not in c.lower() and "vaga" not in c.lower()],
            "caracteristicas": els
        }

    async def _extract_data(self, page: Page, link: str) -> Dict[str, Any]:
        feats = await self.extrair_caracteristicas(page)
        
        nome = await self._safe_text(page, 'a[data-testid="official-store-redirect-link"], .publisher-name')
        preco = await self._safe_text(page, "div.price-info__values-sale .value-item__value, [data-testid='price-value'], .price__value")
        addr = await self._safe_text(page, 'p[data-testid="location-address"], .location__address')
        parsed = parse_endereco(addr) if addr else {}

        # MUDANÇA 3: Filtro melhorado para ignorar SVG (ícones) e pegar apenas JPG/WEBP
        imgs = await page.locator("img").evaluate_all("""els => els
            .map(e => e.src || e.getAttribute('data-src'))
            .filter(src => src && (src.includes('vivareal') || src.includes('olx')) && !src.includes('icon') && !src.endsWith('.svg'))
        """)

        return {
            "nome_anunciante": nome,
            "tipo_transacao": "Venda",
            "preco_venda": preco,
            "endereco": addr,
            "logradouro": parsed.get("logradouro"), "numero": parsed.get("numero"), "bairro": parsed.get("bairro"), "municipio": parsed.get("municipio"), "uf": parsed.get("uf"),
            "metragem": feats.get("metragem"), "quartos": feats.get("quartos"), "banheiros": feats.get("banheiros"), "suites": feats.get("suites"), "vagas": feats.get("vagas"),
            "outros": json.dumps(feats.get("outros", []), ensure_ascii=False), "caracteristicas": feats.get("caracteristicas"),
            "latitude": None, "longitude": None,
            "condominio": await self._safe_text(page, '[data-testid="condoFee"]'), 
            "iptu": await self._safe_text(page, '[data-testid="iptu"]'),
            "qtd_imagens": len(imgs), "urls_imagens": "; ".join(list(set(imgs))[:10]),
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "link": link
        }

    def _save_to_csv(self, data):
        if "urls_imagens" in data: data["urls_imagens"] = f'"{data["urls_imagens"]}"'
        # MUDANÇA 4: encoding="utf-8-sig" aqui também para os appends
        with open(self.csv_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writerow(data)

    async def scrape_batch(self, links: List[str], save_debug: bool = True):
        logger.info(f"🔥 Iniciando SESSÃO HEADFUL (XVFB) para {len(links)} links...")
        
        async with async_playwright() as p:
            browser = await self._setup_browser(p)
            context = await self._setup_context(browser)
            await Stealth().apply_stealth_async(context)
            page = await context.new_page()

            try:
                await page.goto("https://www.vivareal.com.br/", timeout=60000)
                await asyncio.sleep(5)
            except: pass

            for i, link in enumerate(links, 1):
                logger.info(f"[{i}/{len(links)}] >> {link}")
                try:
                    await page.goto(link, referer=page.url, timeout=90000, wait_until="domcontentloaded")
                    await self._human_behavior(page)
                    await page.wait_for_selector("body", timeout=30000)
                    
                    data = await self._extract_data(page, link)
                    
                    if not data['preco_venda'] and not data['endereco']:
                        raise Exception("Dados vazios")

                    self._save_to_csv(data)
                    logger.info("✅ Dados extraídos com sucesso!")
                    await asyncio.sleep(random.uniform(5, 10))

                except Exception as e:
                    logger.warning(f"❌ Erro link {i}: {e}")
                    await asyncio.sleep(5)

            await browser.close()

    async def scrape_link(self, link: str):
        return await self.scrape_batch([link])