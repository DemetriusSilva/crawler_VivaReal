# MVP Imobiliária — Scraper VivaReal

Este repositório contém ferramentas robustas para capturar e processar anúncios do VivaReal usando Playwright com suporte assíncrono.

## Visão Geral

Componentes principais:
- `viva_real/captura_links_async.py` — Captura links de anúncios de forma assíncrona
- `viva_real/scraper_async.py` — Scraper assíncrono para extrair dados dos anúncios
- `viva_real/pipeline_async.py` — Pipeline assíncrono para processar links e extrair dados
- `viva_real/pipeline_full.py` — Pipeline integrado: captura links + extrai dados

## Objetivo

Fornecer um fluxo completo e eficiente para:

1. Capturar links de anúncios a partir de páginas de resultados do VivaReal
2. Processar cada link e extrair informações detalhadas
3. Gravar os resultados em CSVs organizados e auditáveis

## Pré-requisitos

- Python 3.8+ (testado com 3.12)
- pip
- Playwright e seus browsers

## Instalação e Execução

### Opção 1: Com Docker (Recomendado)

O projeto pode ser executado facilmente usando Docker, sem necessidade de configurar o ambiente local:

```bash
# Construir a imagem
docker-compose build

# Executar o pipeline completo (5 páginas por padrão)
docker-compose run --rm scraper

# Capturar apenas links de 2 páginas
docker-compose run --rm scraper --only-links --paginas 2

# Extrair dados de um único anúncio
docker-compose run --rm scraper --link "https://www.vivareal.com.br/imovel/seu-link-aqui"

# Buscar em uma localização específica
docker-compose run --rm scraper --url-base "https://www.vivareal.com.br/venda/sp/campinas/apartamento_residencial/"
```

Os arquivos de saída serão salvos no diretório `output/` do seu sistema local.

### Opção 2: Instalação Local (PowerShell)

Se preferir executar localmente:

```powershell
# 1. Criar e ativar ambiente virtual (recomendado)
python -m venv .venv
.\.venv\Scripts\Activate

# 2. Instalar dependências
python -m pip install -r .\requirements.txt

# 3. Instalar browsers do Playwright
python -m playwright install chromium
```

Notas:
- `requirements.txt` já inclui `playwright` na versão usada pelo projeto.
- Se preferir instalar apenas o Playwright: `python -m pip install playwright` e depois `python -m playwright install chromium`.

Arquitetura e contrato dos dados
--------------------------------
Campos extraídos por `VivaRealScraper` (ordem usada no CSV):

- `nome_anunciante` (string | null)
- `tipo_transacao` (string | null)
- `preco_venda` (string | null)
- `endereco` (string | null)  — campo bruto extraído do anúncio
- `logradouro` (string | null) — parte mapeada do endereço
- `numero` (string | null) — parte mapeada do endereço
- `bairro` (string | null) — parte mapeada do endereço
- `municipio` (string | null) — parte mapeada do endereço
- `uf` (string | null) — parte mapeada do endereço (sigla)
- `metragem` (string | null) — extraída da lista de características (ex: "200 m²")
- `quartos` (string | null) — extraída da lista de características (ex: "3 quartos")
- `banheiros` (string | null) — extraída da lista de características (ex: "2 banheiros")
- `suites` (string | null) — extraída da lista de características (ex: "1 suíte")
- `vagas` (string | null) — extraída da lista de características (ex: "2 vagas")
- `outros` (JSON-encoded list | null) — lista com os itens restantes não classificados (campo texto contendo JSON)
- `latitude` (string | null)
- `longitude` (string | null)
- `condominio` (string | null)
- `iptu` (string | null)
- `caracteristicas` (JSON-encoded list) — lista completa de características (campo texto)
- `qtd_imagens` (int)
- `urls_imagens` (string) — URLs separadas por "; " (o campo é armazenado entre aspas para preservar divisores)
- `data_extracao` (string — ISO datetime)
- `link` (string)

Regras importantes:
- Campos vazios são normalizados para `None`.
- O campo `caracteristicas` é serializado como JSON (texto) no CSV para preservar a lista completa.
- A partir da lista de `caracteristicas` extraída, o scraper tenta identificar e separar valores principais (heuristicamente): `metragem`, `quartos`, `banheiros`, `suites`, `vagas`. Os itens não classificados ficam em `outros` e são serializados como JSON.
- `outros` e `caracteristicas` são armazenados como strings contendo JSON (UTF-8, ensure_ascii=False) para facilitar reimportação posterior.
- Antes de salvar, o scraper valida o registro: se tanto `preco_venda` quanto `endereco` estiverem ausentes, o registro será pulado (não gravado) e um warning será registrado.

## Interface Principal (main.py)

O projeto oferece uma interface unificada através do `main.py` que centraliza todas as operações em uma única ferramenta de linha de comando. Esta interface oferece três modos de operação e várias opções de personalização.

### Opções Globais

Estas opções são válidas para todos os modos de operação:

```
--no-headless     Mostrar navegador durante execução (útil para debug)
--out-dir DIR     Diretório base para arquivos de saída (padrão: output)
--url-base URL    URL base para busca de imóveis 
                  (ex: https://www.vivareal.com.br/venda/sp/sao-paulo/apartamento_residencial/)
```

## Fluxos de Execução

O script `main.py` suporta três modos de operação principais:

### 1. Pipeline Completo (Recomendado)

Executa captura de links + extração de dados em um único comando:

```powershell
python main.py
```

Opções:
- `--paginas N`: Número de páginas para capturar links (padrão: 5)
- `--no-headless`: Mostrar navegador durante execução
- `--out-dir DIR`: Diretório base para arquivos de saída

### 2. Apenas Captura de Links

Captura somente os links dos anúncios sem extrair os dados:

```powershell
python main.py --only-links
```

Opções:
- `--paginas N`: Número de páginas para capturar (padrão: 5)
- `--no-headless`: Mostrar navegador
- `--out-dir DIR`: Diretório base para arquivos de saída

### 3. Extração de Dados de um Único Link

Extrai dados detalhados de um único anúncio específico:

```powershell
python main.py --link "https://www.vivareal.com.br/imovel/seu-link-aqui"
```

Opções:
- `--no-headless`: Mostrar navegador durante execução
- `--out-dir DIR`: Diretório base para arquivos de saída

### Exemplos de Uso

```powershell
# Pipeline completo com 10 páginas
python main.py --paginas 10

# Capturar links de 3 páginas com navegador visível
python main.py --only-links --paginas 3 --no-headless

# Extrair dados de um único anúncio
python main.py --link "https://www.vivareal.com.br/imovel/seu-link-aqui"

# Customizar diretório de saída
python main.py --out-dir meus_dados

# Capturar links em modo debug
python main.py --only-links --no-headless --out-dir debug_links

# Buscar em uma localização específica
python main.py --url-base "https://www.vivareal.com.br/venda/sp/campinas/apartamento_residencial/"

# Buscar casas em vez de apartamentos
python main.py --url-base "https://www.vivareal.com.br/venda/sp/sao-paulo/casa_residencial/"
```

## Estrutura de Arquivos

```
├── main.py                   # Interface principal unificada
├── viva_real/
│   ├── captura_links_async.py    # Captura links (versão assíncrona)
│   ├── scraper_async.py          # Extrai dados (versão assíncrona)
│   ├── pipeline_async.py         # Pipeline de processamento
│   └── pipeline_full.py         # Pipeline integrado
│
└── output/
    ├── links/                    # CSVs com links capturados
    │   └── YYYYMMDD_HHMMSS_N_links.csv
    └── dados/                    # CSVs com dados extraídos
        ├── YYYYMMDD_vivareal.csv         # Dados do pipeline completo
        └── YYYYMMDD_vivareal_single.csv  # Dados de link único
```

### Estrutura dos Arquivos de Saída

1. **Links Capturados** (`output/links/`):
   - Formato: `YYYYMMDD_HHMMSS_N_links.csv`
   - Exemplo: `20251101_083322_122_links.csv`
   - N = número de links capturados

2. **Dados Extraídos** (`output/dados/`):
   - Pipeline completo: `YYYYMMDD_vivareal.csv`
   - Link único: `YYYYMMDD_vivareal_single.csv`

## Melhorias Implementadas

1. **Suporte Assíncrono**
   - Melhor performance e gerenciamento de recursos
   - Compatível com asyncio e event loops

2. **Robustez**
   - Tratamento completo de erros
   - Logging estruturado
   - Garantia de fechamento de recursos

3. **Organização**
   - Código modular e bem documentado
   - Type hints para melhor manutenção
   - Classes bem estruturadas

4. **Facilidade de Uso**
   - CLI intuitiva com opções úteis
   - Pipeline completo integrado
   - Modos headless e debug

## Observabilidade e Logs

- Logging estruturado com níveis (INFO/WARNING/ERROR)
- Timestamps e contexto em todas as mensagens
- Rastreamento de progresso e estatísticas

## Boas Práticas

1. **Performance**
   - Execução assíncrona eficiente
   - Gerenciamento otimizado de recursos
   - Cache de seletores e elementos

2. **Manutenção**
   - Código tipado e documentado
   - Funções e classes coesas
   - Separação de responsabilidades

3. **Produção**
   - Tratamento robusto de erros
   - Fechamento garantido de recursos
   - Logs estruturados para monitoramento

## Debug e Troubleshooting

1. **Problemas Comuns**
   - Browser não abre: Verifique instalação do Playwright
   - Seletores falham: Use `--no-headless` para debug visual
   - Erros de rede: Verifique conexão e timeouts

2. **Dicas**
   - Use `--no-headless` para ver a execução
   - Verifique logs para rastrear problemas
   - Comece com `--limite-links` baixo para testes

## Licença

Este código é uma PoC para fins educacionais. Ao usar web scrapers, verifique:
- Termos de Serviço do site alvo
- Leis e regulamentos aplicáveis
- Boas práticas de scraping responsável
