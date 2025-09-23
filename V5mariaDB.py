import os, re, json, unicodedata, requests, feedparser, pymysql
from datetime import datetime, UTC
from urllib.parse import urlparse

# ---------- spaCy (optionnel) ----------
try:
    import spacy
except Exception:
    spacy = None

def _load_model(name):
    if not spacy:
        return None
    try:
        return spacy.load(name)
    except Exception:
        return None

nlp_fr = _load_model("fr_core_news_md") or _load_model("fr_core_news_sm")
nlp_en = _load_model("en_core_web_md") or _load_model("en_core_web_sm")

# ---------- Extraction plein texte ----------
try:
    import trafilatura
except Exception:
    trafilatura = None
from bs4 import BeautifulSoup
PARSER = "lxml"
try:
    import lxml  # noqa
except Exception:
    PARSER = "html.parser"

def extract_fulltext(url: str, timeout: int = 20) -> str | None:
    if trafilatura:
        try:
            downloaded = trafilatura.fetch_url(url, timeout=timeout)
            if downloaded:
                text = trafilatura.extract(
                    downloaded, include_comments=False, include_tables=False, no_fallback=False
                )
                if text and text.strip():
                    return text.strip()
        except Exception:
            pass
    try:
        r = requests.get(url, headers={"User-Agent": feedparser.USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, PARSER)
        for tag in soup(["script","style","nav","header","footer","aside","form","noscript","figure"]):
            tag.decompose()
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text = "\n\n".join(p for p in paras if p)
        return text.strip() if text else None
    except Exception:
        return None

# ---------- Config ----------
RSS_URLS = [
    # BBC
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "http://feeds.bbci.co.uk/news/business/rss.xml",
    "http://feeds.bbci.co.uk/news/technology/rss.xml",
    "http://feeds.bbci.co.uk/news/world/asia/rss.xml",
    "http://feeds.bbci.co.uk/news/world/europe/rss.xml",

    # Le Monde
    "https://www.lemonde.fr/rss/une.xml",
    "https://www.lemonde.fr/international/rss_full.xml",
    "https://www.lemonde.fr/economie/rss_full.xml",
    "https://www.lemonde.fr/planete/rss_full.xml",
    "https://www.lemonde.fr/pixels/rss_full.xml",

    # WSJ
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://feeds.a.dj.com/rss/RSSWorldNews.xml",

    # Guardian UK
    "https://www.theguardian.com/uk/rss",

    # France/Europe
    "https://www.francetvinfo.fr/france.rss",
    "https://www.rfi.fr/fr/rss",
    "https://www.bfmtv.com/rss/economie/",
]

feedparser.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python/feedparser"
HEADERS = {"User-Agent": feedparser.USER_AGENT}

# ---------- Connexion MariaDB ----------
MDB = {
    "host": "localhost",
    "user": "newsuser",
    "password": "newspass",
    "database": "news",
    "charset": "utf8mb4",
    "autocommit": True,
}
def get_conn():
    return pymysql.connect(**MDB)

# ---------- Métadonnées éditeur ----------
TLD_TO_COUNTRY = {
    ".fr":"FR",".de":"DE",".es":"ES",".it":"IT",".be":"BE",".dk":"DK",
    ".co.uk":"GB",".uk":"GB",".com":"", ".org":"", ".net":""
}
DOMAIN_COUNTRY_OVERRIDE = {
    "lemonde.fr":"FR","lesechos.fr":"FR","bbc.co.uk":"GB","bbc.com":"GB"
}
def publisher_meta(link: str):
    dom = (urlparse(link).netloc or "").lower()
    if dom.startswith("www."): dom = dom[4:]
    if dom in DOMAIN_COUNTRY_OVERRIDE:
        return dom, DOMAIN_COUNTRY_OVERRIDE[dom]
    for tld, cc in TLD_TO_COUNTRY.items():
        if dom.endswith(tld):
            return dom, cc
    return dom, ""

def update_article_publisher(con, article_id: int, link: str, lang: str|None):
    dom, cc = publisher_meta(link)
    cur = con.cursor()
    cur.execute("""
        UPDATE articles
        SET publisher_domain=%s,
            publisher_country=%s,
            lang=COALESCE(%s, lang)
        WHERE id=%s
    """, (dom, cc, lang, article_id))

# ---------- RSS ----------
def parse_feed(url: str):
    f = feedparser.parse(url)
    if len(f.entries) == 0 or getattr(f, "bozo", 0):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            f = feedparser.parse(r.content)
        except Exception:
            pass
    return f

def insert_article_return_id(con, row):
    cur = con.cursor()
    # équivalent SQLite "INSERT OR IGNORE"
    cur.execute("""
        INSERT IGNORE INTO articles (source, title, date, link, summary, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row["source"], row["title"], row["date"], row["link"], row["summary"], row["fetched_at"]))
    cur.execute("SELECT id FROM articles WHERE link=%s", (row["link"],))
    r = cur.fetchone()
    return r[0] if r else None

# ---------- NER + synthèse inline ----------
COUNTRY_NAMES = {
    # EN
    "france","germany","spain","italy","belgium","denmark","united kingdom","uk","russia",
    "china","taiwan","united states","usa","u.s.","u.s.a.","canada","mexico",
    # FR
    "france","allemagne","espagne","italie","belgique","danemark","royaume-uni","russie",
    "chine","taïwan","etats-unis","états-unis","canada","mexique",
}
def _norm(s: str) -> str:
    import unicodedata
    return re.sub(r"\s+"," ", unicodedata.normalize("NFKC", (s or "").strip()))
def _dedup(lst):
    seen, out = set(), []
    for x in lst or []:
        if not x: continue
        k = x.lower()
        if k not in seen:
            seen.add(k); out.append(x)
    return out
def _extract_presidents(text: str, persons: list[str]) -> list[str]:
    t = " " + (text or "").lower() + " "
    out = []
    for p in persons or []:
        n = p.lower()
        if re.search(rf"(président|president)[^\.]{{0,80}}\b{re.escape(n)}\b", t) or \
           re.search(rf"\b{re.escape(n)}\b[^\.]{{0,80}}(président|president)", t):
            out.append(p)
    return _dedup(out)

def choose_nlp_doc(text: str, link: str|None):
    if nlp_fr is None and nlp_en is None:
        return None, None
    dom = (urlparse(link or "").netloc or "").lower()
    if "bbc" in dom and nlp_en:
        return nlp_en(text), "en"
    if any(k in dom for k in ("lemonde.fr","lesechos.fr")) and nlp_fr:
        return nlp_fr(text), "fr"
    docs = []
    if nlp_fr: docs.append(("fr", nlp_fr(text)))
    if nlp_en: docs.append(("en", nlp_en(text)))
    if not docs:
        return None, None
    lang, doc = max(docs, key=lambda p: len(p[1].ents))
    return doc, lang

def summarize_inline(con, article_id: int, full_text: str, link: str):
    if not article_id:
        return
    doc, lang = choose_nlp_doc(full_text or "", link or "")
    update_article_publisher(con, article_id, link, lang)

    people, gpes, locs, events = [], [], [], []
    if doc is not None and getattr(doc, "ents", None):
        for ent in doc.ents:
            txt = _norm(ent.text)
            if not txt: 
                continue
            if ent.label_ == "PERSON": people.append(txt)
            elif ent.label_ == "GPE":  gpes.append(txt)
            elif ent.label_ == "LOC":  locs.append(txt)
            elif ent.label_ == "EVENT": events.append(txt)
    if not (people or gpes or locs or events) and full_text:
        ft = full_text.lower()
        for c in COUNTRY_NAMES:
            if re.search(rf"\b{re.escape(c)}\b", ft):
                gpes.append(c.title())

    countries, cities = [], []
    for g in gpes:
        if g.lower() in COUNTRY_NAMES: countries.append(g)
        else: cities.append(g)

    people    = _dedup(people)
    countries = _dedup(countries)
    cities    = _dedup(cities)
    events    = _dedup(events)
    presidents = _extract_presidents(full_text, people)

    cur = con.cursor()
    cur.execute("""
        UPDATE articles
        SET people=%s, countries=%s, cities=%s, events=%s, presidents=%s
        WHERE id=%s
    """, (json.dumps(people, ensure_ascii=False),
          json.dumps(countries, ensure_ascii=False),
          json.dumps(cities, ensure_ascii=False),
          json.dumps(events, ensure_ascii=False),
          json.dumps(presidents, ensure_ascii=False),
          article_id))

def update_fulltext(con, article_id, fulltext):
    cur = con.cursor()
    cur.execute("""
        UPDATE articles
        SET content=%s, content_len=%s, content_fetched_at=%s
        WHERE id=%s
    """, (fulltext, len(fulltext), datetime.now(UTC).isoformat(timespec="seconds"), article_id))

# ---------- Main ----------
def main():
    total_new = 0
    con = get_conn()
    try:
        for url in RSS_URLS:
            f = parse_feed(url)
            source_name = f.feed.get("title", url)
            print(f"Titre du flux : {source_name}")
            print("Nombre d'articles récupérés :", len(f.entries))

            added = 0
            for entry in f.entries:
                row = {
                    "source": source_name,
                    "title": entry.get("title", "") or "",
                    "date": entry.get("published", entry.get("updated", "")) or "",
                    "link": entry.get("link", "") or "",
                    "summary": entry.get("summary", entry.get("description", "")) or "",
                    "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
                if not row["link"]:
                    continue

                article_id = insert_article_return_id(con, row)
                if article_id:
                    added += 1

                fulltext = extract_fulltext(row["link"])
                if fulltext:
                    update_fulltext(con, article_id, fulltext)

                text_for_ner = f"{row['title']} {row['summary']} {(fulltext or '')[:2000]}".strip()
                summarize_inline(con, article_id, text_for_ner, row["link"])

            total_new += added
            print(f"+{added} nouveaux depuis ce flux\n")
    finally:
        con.close()
    print(f"Terminé. {total_new} nouveaux articles insérés en MariaDB.")

if __name__ == "__main__":
    main()
