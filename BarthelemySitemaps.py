# rss_to_db.py
import feedparser, requests, sqlite3, os
from datetime import datetime, UTC, timedelta
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin
import re
import time

import spacy
from urllib.parse import urlparse

def _load_model(name):
    try:
        return spacy.load(name)
    except Exception:
        return None

# Priorité : md > sm ; FR & EN ; fallback multi-langues
nlp_fr = _load_model("fr_core_news_md") or _load_model("fr_core_news_sm")
nlp_en = _load_model("en_core_web_md") or _load_model("en_core_web_sm")

# 1) Configuration des sources
RSS_URLS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.lemonde.fr/rss/une.xml",
    "https://syndication.lesechos.fr/rss/rss_id_finance.xml"
]

# Sitemaps à explorer (ajoutez-en selon vos besoins)
SITEMAP_CONFIGS = [
    {
        "url": "https://www.lemonde.fr/sitemap_news.xml",
        "domain": "lemonde.fr",
        "max_age_days": 30,  # Ne récupère que les articles récents
        "delay": 1  # Délai entre requêtes (politesse)
    },
    {
        "url": "https://www.bbc.com/sitemaps/https-sitemap-com-news-1.xml",
        "domain": "bbc.com", 
        "max_age_days": 14,
        "delay": 0.5
    },
    {
        "url": "https://www.lesechos.fr/sitemap_news.xml",
        "domain": "lesechos.fr",
        "max_age_days": 21,
        "delay": 1
    }
]

# 2) DB locale (SQLite)
DB_PATH = os.path.join(os.path.dirname(__file__), "NewsSitemaps.db")
print("DB utilisée :", os.path.abspath(DB_PATH))

# 3) Feedparser: user-agent (certains sites bloquent sinon)
feedparser.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python/feedparser"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Configuration pour les métadonnées éditeur
TLD_TO_COUNTRY = {
    ".fr":"FR",".de":"DE",".es":"ES",".it":"IT",".be":"BE",".dk":"DK",
    ".co.uk":"GB",".uk":"GB",".com":"",".org":"",".net":""
}
DOMAIN_COUNTRY_OVERRIDE = {
    "lemonde.fr":"FR","lesechos.fr":"FR","bbc.co.uk":"GB","bbc.com":"GB"
}

# Configuration des topics
TOPIC_RULES = {
    "Markets": ["bourse","equity","stocks","indice","obligations","yield","volatilité","ETF"],
    "Macro":   ["inflation","gdp","cpi","pmi","récession","croissance","emploi","chômage","BCE","FED","banque centrale"],
    "Energy":  ["pétrole","gaz","opec","opep","brent","énergie","nucléaire"],
    "Tech":    ["ia","ai","nvidia","semi","puce","cloud","logiciel","cyber"],
    "Geo":     ["ukraine","russia","china","beijing","taiwan","otan","nato","conflit","sanctions"],
}

def publisher_meta(link: str):
    dom = (urlparse(link).netloc or "").lower()
    if dom.startswith("www."): dom = dom[4:]
    if dom in DOMAIN_COUNTRY_OVERRIDE: return dom, DOMAIN_COUNTRY_OVERRIDE[dom]
    for tld, cc in TLD_TO_COUNTRY.items():
        if dom.endswith(tld): return dom, cc
    return dom, ""

def update_article_publisher(con, article_id: int, link: str, lang: str|None):
    dom, cc = publisher_meta(link)
    con.execute("""
        UPDATE articles
        SET publisher_domain=?, publisher_country=?, lang=COALESCE(?, lang)
        WHERE id=?
    """, (dom, cc, lang, article_id))

def detect_topics(text: str):
    t = text.lower()
    out = []
    for topic, keys in TOPIC_RULES.items():
        hits = sum(k in t for k in keys)
        if hits: out.append((topic, min(1.0, 0.2*hits)))
    return out

def store_topics(con, article_id, text):
    topics = detect_topics(text)
    if topics:
        con.executemany("""
            INSERT OR IGNORE INTO article_topics(article_id, topic, score, source)
            VALUES (?,?,?,?)
        """, [(article_id, tp, sc, "rules") for tp, sc in topics])

def choose_nlp_doc(text: str, link: str|None):
    # Heuristique simple par domaine
    dom = (urlparse(link).netloc or "").lower()
    if "bbc" in dom and nlp_en: return nlp_en(text), "en"
    if any(k in dom for k in ("lemonde.fr","lesechos.fr")) and nlp_fr: return nlp_fr(text), "fr"
    # sinon, meilleur des deux
    docs = []
    if nlp_fr: docs.append(("fr", nlp_fr(text)))
    if nlp_en: docs.append(("en", nlp_en(text)))
    if not docs: return None, None
    lang, doc = max(docs, key=lambda p: len(p[1].ents))
    return doc, lang

def add_column_if_missing(con, table, column, sql_type):
    cols = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")
        

def ensure_db():
    with sqlite3.connect(DB_PATH) as con:
        # table articles (création si absente)
        con.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                title TEXT,
                date TEXT,
                link TEXT UNIQUE,
                summary TEXT,
                fetched_at TEXT
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_articles_date_source ON articles(date, source);")

        # ajouter les nouvelles colonnes si manquantes
        add_column_if_missing(con, "articles", "publisher_domain",  "TEXT")
        add_column_if_missing(con, "articles", "publisher_country", "TEXT")
        add_column_if_missing(con, "articles", "lang",              "TEXT")
        add_column_if_missing(con, "articles", "source_type",       "TEXT DEFAULT 'rss'")
        add_column_if_missing(con, "articles", "people",     "TEXT")

        # table entities
        con.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                text TEXT,
                label TEXT,
                start INTEGER,
                "end" INTEGER,
                FOREIGN KEY(article_id) REFERENCES articles(id)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_entities_article ON entities(article_id);")
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_unique
            ON entities(article_id, text, label, start, "end");
        """)

        # table topics
        con.execute("""
            CREATE TABLE IF NOT EXISTS article_topics (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              article_id INTEGER NOT NULL,
              topic TEXT,
              score REAL,
              source TEXT,
              UNIQUE(article_id, topic),
              FOREIGN KEY(article_id) REFERENCES articles(id)
            )
        """)

# === FONCTIONS SITEMAP ===

def fetch_sitemap(url: str, delay: float = 0):
    """Récupère et parse un sitemap XML"""
    if delay > 0:
        time.sleep(delay)
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return ET.fromstring(response.content)
    except Exception as e:
        print(f"Erreur lors de la récupération du sitemap {url}: {e}")
        return None

def parse_sitemap_index(root):
    """Parse un sitemap index pour récupérer les sous-sitemaps"""
    sitemaps = []
    for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
        loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
        if loc is not None:
            sitemaps.append(loc.text)
    return sitemaps

def parse_sitemap_urls(root, max_age_days: int = None):
    """Parse les URLs d'un sitemap avec filtrage par date"""
    urls = []
    cutoff_date = None
    if max_age_days:
        cutoff_date = datetime.now(UTC) - timedelta(days=max_age_days)
    
    # Sitemap standard
    for url_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
        loc = url_elem.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
        lastmod = url_elem.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
        
        if loc is not None:
            url = loc.text
            
            # Filtrage par date si spécifié
            if cutoff_date and lastmod is not None:
                try:
                    # Parse la date (formats ISO 8601 courants)
                    date_str = lastmod.text
                    if 'T' in date_str:
                        article_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    else:
                        article_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
                    
                    if article_date < cutoff_date:
                        continue
                except:
                    pass  # En cas d'erreur de parsing, on inclut l'URL
            
            urls.append(url)
    
    # Sitemap News (format Google News)
def parse_sitemap_urls(root, max_age_days: int = None):
    """Parse les URLs d'un sitemap (standard + news) avec filtrage par date, compatible ElementTree."""
    SM = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    NEWS = "{http://www.google.com/schemas/sitemap-news/0.9}"

    urls = []
    cutoff_date = None
    if max_age_days:
        cutoff_date = datetime.now(UTC) - timedelta(days=max_age_days)

    # On parcourt chaque <url>
    for url_el in root.findall(f".//{SM}url"):
        loc_el = url_el.find(f"{SM}loc")
        if loc_el is None or not (loc_el.text and loc_el.text.strip()):
            continue
        loc = loc_el.text.strip()

        # lastmod éventuel
        lastmod_el = url_el.find(f"{SM}lastmod")
        lastmod_txt = (lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else "")

        # Présence éventuelle d'un bloc <news:news> (on n'utilise pas getparent)
        news_el = url_el.find(f"{NEWS}news")
        news_pubdate = ""
        if news_el is not None:
            # Tente d'extraire une date de publication news si présente
            pub_el = news_el.find(f"{NEWS}publication_date")
            if pub_el is not None and pub_el.text:
                news_pubdate = pub_el.text.strip()

        # --- Filtrage par date si demandé (on prend la meilleure date dispo) ---
        if cutoff_date:
            date_text = news_pubdate or lastmod_txt
            if date_text:
                try:
                    # Formats simples ISO
                    if "T" in date_text:
                        d = datetime.fromisoformat(date_text.replace("Z", "+00:00"))
                    else:
                        d = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=UTC)
                    if d < cutoff_date:
                        continue
                except Exception:
                    # format inconnu -> on ne filtre pas
                    pass

        urls.append(loc)

    return urls


def is_news_url(url: str, domain: str) -> bool:
    """Heuristiques pour identifier si une URL est un article de news"""
    url_lower = url.lower()
    
    # Patterns courants pour les articles de news
    news_indicators = [
        '/news/', '/article/', '/articles/', '/actualite/', '/actualites/',
        '/politique/', '/economie/', '/international/', '/monde/',
        '/business/', '/finance/', '/tech/', '/technology/',
        '/sport/', '/culture/', '/societe/'
    ]
    
    # Patterns de dates (YYYY/MM/DD, YYYY-MM-DD, etc.)
    date_patterns = [
        r'/\d{4}/\d{2}/\d{2}/',
        r'/\d{4}-\d{2}-\d{2}/',
        r'/\d{4}/\d{2}/'
    ]
    
    # Vérifier les indicateurs de news
    if any(indicator in url_lower for indicator in news_indicators):
        return True
    
    # Vérifier les patterns de dates
    if any(re.search(pattern, url) for pattern in date_patterns):
        return True
    
    # Exclusions communes
    exclusions = [
        '/tag/', '/tags/', '/category/', '/author/', '/page/',
        '.pdf', '.jpg', '.png', '.gif', '.css', '.js',
        '/search', '/contact', '/about', '/legal'
    ]
    
    if any(exclusion in url_lower for exclusion in exclusions):
        return False
    
    return False  # Par défaut, ne pas inclure

def extract_article_content(url: str, domain: str) -> dict:
    """Extrait le contenu basique d'un article depuis son URL"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        content = response.text
        
        # Extraction basique avec regex (à améliorer selon les sites)
        title_match = re.search(r'<title[^>]*>(.*?)</title>', content, re.IGNORECASE | re.DOTALL)
        title = ""
        if title_match:
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            # Nettoyer les suffixes courants
            for suffix in [' - Le Monde', ' - BBC News', ' - Les Echos']:
                if title.endswith(suffix):
                    title = title[:-len(suffix)]
        
        # Extraction de métadonnées Open Graph / Twitter Cards
        og_description = re.search(r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']*)["\']', content, re.IGNORECASE)
        twitter_desc = re.search(r'<meta[^>]*name=["\']twitter:description["\'][^>]*content=["\']([^"\']*)["\']', content, re.IGNORECASE)
        meta_desc = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)["\']', content, re.IGNORECASE)
        
        description = ""
        if og_description:
            description = og_description.group(1)
        elif twitter_desc:
            description = twitter_desc.group(1)
        elif meta_desc:
            description = meta_desc.group(1)
        
        # Date de publication
        date_published = re.search(r'<meta[^>]*property=["\']article:published_time["\'][^>]*content=["\']([^"\']*)["\']', content, re.IGNORECASE)
        pub_date = ""
        if date_published:
            pub_date = date_published.group(1)
        
        return {
            "title": title,
            "description": description,
            "date": pub_date,
            "url": url
        }
        
    except Exception as e:
        print(f"Erreur extraction contenu {url}: {e}")
        return {"title": "", "description": "", "date": "", "url": url}

def process_sitemap(config: dict) -> int:
    """Traite un sitemap selon sa configuration"""
    print(f"\n=== Traitement du sitemap: {config['domain']} ===")
    
    root = fetch_sitemap(config['url'], config.get('delay', 0))
    if root is None:
        return 0
    
    # Vérifier si c'est un sitemap index
    sitemaps = parse_sitemap_index(root)
    
    all_urls = []
    if sitemaps:
        print(f"Sitemap index détecté avec {len(sitemaps)} sous-sitemaps")
        for sitemap_url in sitemaps[:5]:  # Limiter pour éviter la surcharge
            print(f"  Traitement: {sitemap_url}")
            sub_root = fetch_sitemap(sitemap_url, config.get('delay', 0))
            if sub_root:
                urls = parse_sitemap_urls(sub_root, config.get('max_age_days'))
                all_urls.extend(urls)
    else:
        all_urls = parse_sitemap_urls(root, config.get('max_age_days'))
    
    # Filtrer les URLs pertinentes
    news_urls = [url for url in all_urls if is_news_url(url, config['domain'])]
    print(f"URLs trouvées: {len(all_urls)}, URLs news: {len(news_urls)}")
    
    # Traiter les articles
    added_count = 0
    with sqlite3.connect(DB_PATH) as con:
        for i, url in enumerate(news_urls[:100]):  # Limiter pour les tests
            if i % 10 == 0:
                print(f"  Progression: {i}/{min(100, len(news_urls))}")
            
            # Vérifier si l'article existe déjà
            existing = con.execute("SELECT id FROM articles WHERE link = ?", (url,)).fetchone()
            if existing:
                continue
            
            # Extraire le contenu
            article_data = extract_article_content(url, config['domain'])
            if not article_data['title']:
                continue
            
            # Préparer les données pour insertion
            row = {
                "source": f"sitemap-{config['domain']}",
                "title": article_data['title'],
                "date": article_data['date'] or datetime.now(UTC).isoformat(),
                "link": url,
                "summary": article_data['description'],
                "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "source_type": "sitemap"
            }
            
            # Insérer l'article
            article_id = insert_article_return_id(con, row)
            if article_id:
                added_count += 1
                
                # Traitement NER et enrichissement
                text_for_ner = f"{row['title']} {row['summary']}".strip()
                doc, lang = choose_nlp_doc(text_for_ner, url)
                
                if doc and doc.ents:
                    rows_ner = [(article_id, ent.text, ent.label_, ent.start_char, ent.end_char) for ent in doc.ents]
                    con.executemany("""
                        INSERT OR IGNORE INTO entities (article_id, text, label, start, "end")
                        VALUES (?,?,?,?,?)
                    """, rows_ner)
                
                update_article_publisher(con, article_id, url, lang)
                store_topics(con, article_id, text_for_ner)
            
            # Respecter le délai entre requêtes
            if config.get('delay', 0) > 0:
                time.sleep(config['delay'])
    
    print(f"✓ {added_count} nouveaux articles ajoutés depuis {config['domain']}")
    return added_count

# === FONCTIONS EXISTANTES (légèrement modifiées) ===

def parse_feed(url: str):
    """Essaie feedparser; si vide, retente avec requests (User-Agent)."""
    f = feedparser.parse(url)
    if len(f.entries) == 0 or getattr(f, "bozo", 0):
        try:
            headers = {"User-Agent": feedparser.USER_AGENT}
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            f = feedparser.parse(r.content)
        except Exception:
            pass
    return f

def insert_article_return_id(con, row):
    """Insert OR IGNORE l'article et retourne son id (nouveau ou existant)."""
    cur = con.execute("""
        INSERT OR IGNORE INTO articles (source, title, date, link, summary, fetched_at, source_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (row["source"], row["title"], row["date"], row["link"], row["summary"], 
          row["fetched_at"], row.get("source_type", "rss")))
    
    if cur.rowcount == 1:
        return cur.lastrowid
    # déjà présent : récupérer l'id existant
    r = con.execute("SELECT id FROM articles WHERE link = ?", (row["link"],)).fetchone()
    return r[0] if r else None

def main():
    ensure_db()
    total_new = 0
    
    # 1. Traitement des flux RSS (existant)
    print("=== TRAITEMENT DES FLUX RSS ===")
    with sqlite3.connect(DB_PATH) as con:
        for url in RSS_URLS:
            f = parse_feed(url)
            source_name = f.feed.get("title", url)
            print(f"Titre du flux : {source_name}")
            print("Nombre d'articles récupérés :", len(f.entries))

            added = 0
            for entry in f.entries:
                row = {
                    "source": source_name,
                    "title": entry.get("title", ""),
                    "date": entry.get("published", entry.get("updated", "")),
                    "link": entry.get("link", ""),
                    "summary": entry.get("summary", entry.get("description", "")),
                    "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "source_type": "rss"
                }
                if not row["link"]:
                    continue

                before = con.total_changes
                article_id = insert_article_return_id(con, row)
                if con.total_changes > before:
                    added += 1

                # Traitement NER et enrichissement
                text_for_ner = f"{row['title']} {row['summary']}".strip()
                doc, lang = choose_nlp_doc(text_for_ner, row["link"])

                if doc and doc.ents:
                    rows_ner = [(article_id, ent.text, ent.label_, ent.start_char, ent.end_char) for ent in doc.ents]
                    con.executemany("""
                        INSERT OR IGNORE INTO entities (article_id, text, label, start, "end")
                        VALUES (?,?,?,?,?)
                    """, rows_ner)

                update_article_publisher(con, article_id, row["link"], lang)
                store_topics(con, article_id, text_for_ner)

            total_new += added
            print(f"+{added} nouveaux depuis ce flux\n")

    # 2. Traitement des sitemaps (nouveau)
    print("\n=== TRAITEMENT DES SITEMAPS ===")
    for config in SITEMAP_CONFIGS:
        sitemap_added = process_sitemap(config)
        total_new += sitemap_added

    print(f"\n🎉 Terminé. {total_new} nouveaux articles au total insérés dans {DB_PATH}.")

if __name__ == "__main__":
    main()