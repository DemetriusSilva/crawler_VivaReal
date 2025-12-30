FROM python:3.12-slim

# Evita interação com usuário durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependências necessárias para o Playwright
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    # Dependências básicas
    wget \
    ca-certificates \
    gnupg \
    # Dependências gráficas
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
    xvfb \
    # Limpa o cache do apt
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos do projeto
COPY requirements.txt .
COPY main.py .
COPY viva_real ./viva_real

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Instala os navegadores do Playwright (com dependências do sistema)
RUN python -m playwright install-deps chromium \
    && python -m playwright install chromium

# Cria diretórios necessários e ajusta permissões
RUN mkdir -p output/links output/dados \
    && chmod -R 777 /app/output

# Define as variáveis de ambiente
ENV PYTHONUNBUFFERED=1

# Comando padrão (pode ser sobrescrito no docker-compose ou na linha de comando)
ENTRYPOINT ["python", "main.py"]
CMD ["--paginas", "5"]