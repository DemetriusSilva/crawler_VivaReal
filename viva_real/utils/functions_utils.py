def parse_price_info(page):
    prices = {}
    items = page.locator("div.price-info__values div.value-item")
    for i in range(items.count()):
        title_el = items.nth(i).locator("p.value-item__title")
        value_el = items.nth(i).locator("p.value-item__value")
        title = title_el.inner_text().strip() if title_el.count() else None
        value = value_el.inner_text().strip() if value_el.count() else None
        if title and value:
            key = title.lower().replace(" ", "_").replace("ç", "c")  # normaliza o título
            prices[key] = value
    return prices


def parse_endereco(endereco: str) -> dict:
    """Parseia um campo de endereço livre em componentes.

    Retorna um dicionário com chaves: logradouro, numero, bairro, municipio, uf.
    Valores ausentes serão retornados como None.
    """
    import re

    if not endereco:
        return {"logradouro": None, "numero": None, "bairro": None, "municipio": None, "uf": None}

    s = endereco.strip()
    # split by dash groups (separador comum antes do bairro/municipio/UF)
    parts = re.split(r"\s*-\s*", s)

    # detect UF (última parte com 2 letras)
    uf = None
    if parts and re.fullmatch(r"[A-Za-z]{2}", parts[-1].strip()):
        uf = parts.pop(-1).strip().upper()

    logradouro = numero = bairro = municipio = None

    street_indicator = re.compile(r"\b(rua|r\.|avenida|av\.|av|praça|praca|travessa|alameda|rodovia|rod\.|estrada|largo|al\.|rua)\b", re.I)

    # helper to clean tokens
    def _tokens(text: str):
        return [p.strip() for p in text.split(",") if p.strip()]

    # If we have at least two dash-separated parts, first is commonly logradouro and second contains bairro/municipio
    if parts:
        first = parts[0].strip()
        second = parts[1].strip() if len(parts) >= 2 else None

        # Decide if 'first' is a street (contains street keywords or explicit number)
        looks_like_street = bool(street_indicator.search(first)) or bool(re.search(r"\d", first))

        if looks_like_street:
            # try to split "logradouro, numero" or trailing number
            t = _tokens(first)
            if len(t) >= 2 and re.search(r"\d", t[-1]):
                numero = t[-1]
                logradouro = ", ".join(t[:-1])
            else:
                m = re.match(r"^(.*?)[,\s]+(\d[\w/-]*)$", first)
                if m:
                    logradouro = m.group(1).strip()
                    numero = m.group(2).strip()
                else:
                    logradouro = first

            # parse second part for bairro/municipio
            if second:
                sc = _tokens(second)
                if len(sc) >= 2:
                    bairro = sc[0]
                    municipio = sc[1]
                elif len(sc) == 1:
                    # if only one token, decide if it's bairro or municipio by length
                    token = sc[0]
                    if len(token.split()) <= 3:
                        bairro = token
                    else:
                        municipio = token

        else:
            # first doesn't look like a street -> probably bairro or "bairro, municipio"
            fc = _tokens(first)
            if len(fc) >= 2:
                bairro = fc[0]
                municipio = fc[1]
            else:
                bairro = fc[0] if fc else None

            # if there's a second part, try to fill municipio from it
            if second and municipio is None:
                sc = _tokens(second)
                if len(sc) >= 1:
                    municipio = sc[-1]

    # Fallback: if municipio still None, try to pick last comma-separated token across all parts
    if municipio is None:
        combined = ", ".join(parts)
        combined_tokens = [p.strip() for p in combined.split(",") if p.strip()]
        if len(combined_tokens) >= 2:
            municipio = combined_tokens[-1]
            if bairro is None and len(combined_tokens) >= 3:
                bairro = combined_tokens[-2]

    # normalize empty strings to None
    def _none(x):
        return x if x and x.strip() else None

    return {
        "logradouro": _none(logradouro),
        "numero": _none(numero),
        "bairro": _none(bairro),
        "municipio": _none(municipio),
        "uf": _none(uf),
    }