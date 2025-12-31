FROM python:3.12-slim

# Evita interação com usuário durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependências necessárias para o Playwright e Xvfb
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    gnupg \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxshmfence1 \
    # Adicionado xauth aqui
    xvfb \
    xauth \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia arquivos
COPY requirements.txt .
COPY main.py .
COPY viva_real ./viva_real

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Instala browsers
RUN python -m playwright install-deps chromium \
    && python -m playwright install chromium

# Cria diretórios
RUN mkdir -p output/links output/dados \
    && chmod -R 777 /app/output

ENV PYTHONUNBUFFERED=1

# ENTRYPOINT ATUALIZADO:
# --auto-servernum: Evita conflito se o ID 99 já estiver em uso
# -ac: Desativa controle de acesso (Access Control) para evitar erro de permissão no Xauth
ENTRYPOINT ["sh", "-c", "xvfb-run --auto-servernum --server-args='-screen 0 1920x1080x24 -ac' python main.py --no-headless \"$@\"", "--"]