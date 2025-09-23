# rss_to_db.py
import feedparser, requests, sqlite3, os
from datetime import datetime, UTC

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




# 1) Flux RSS (ajoute/retire ce que tu veux)
RSS_URLS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.lemonde.fr/rss/une.xml",
    "https://syndication.lesechos.fr/rss/rss_id_finance.xml"
]

# 2) DB locale (SQLite)
DB_PATH = "news.db"

# 3) Feedparser: user-agent (certains sites bloquent sinon)
feedparser.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python/feedparser"

#Creation de news db si elle n'existe pas 


from urllib.parse import urlparse

TLD_TO_COUNTRY = {
    ".fr":"FR",".de":"DE",".es":"ES",".it":"IT",".be":"BE",".dk":"DK",
    ".co.uk":"GB",".uk":"GB",".com":"",".org":"",".net":""
}
DOMAIN_COUNTRY_OVERRIDE = {
    "lemonde.fr":"FR","lesechos.fr":"FR","bbc.co.uk":"GB","bbc.com":"GB"
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


TOPIC_RULES = {
    "Markets": ["bourse","equity","stocks","indice","obligations","yield","volatilité","ETF"],
    "Macro":   ["inflation","gdp","cpi","pmi","récession","croissance","emploi","chômage","BCE","FED","banque centrale"],
    "Energy":  ["pétrole","gaz","opec","opep","brent","énergie","nucléaire"],
    "Tech":    ["ia","ai","nvidia","semi","puce","cloud","logiciel","cyber"],
    "Geo":     ["ukraine","russia","china","beijing","taiwan","otan","nato","conflit","sanctions"],
}
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



# --- juste sous DB_PATH ---
DB_PATH = os.path.join(os.path.dirname(__file__), "news.db")  # évite d'ouvrir un autre fichier par erreur
print("DB utilisée :", os.path.abspath(DB_PATH))

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
        add_column_if_missing(con, "articles", "countries",  "TEXT")
        add_column_if_missing(con, "articles", "cities",     "TEXT")
        add_column_if_missing(con, "articles", "events",     "TEXT")
        add_column_if_missing(con, "articles", "presidents", "TEXT")
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

        # table topics (utilisée par store_topics)
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

def insert_article(con, row):
    """Insert avec déduplication sur link (UNIQUE)."""
    con.execute("""
        INSERT OR IGNORE INTO articles (source, title, date, link, summary, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (row["source"], row["title"], row["date"], row["link"], row["summary"], row["fetched_at"]))


from urllib.parse import urlparse

def insert_article_return_id(con, row):
    """
    Insert OR IGNORE l'article et retourne son id (nouveau ou existant).
    """
    cur = con.execute("""
        INSERT OR IGNORE INTO articles (source, title, date, link, summary, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (row["source"], row["title"], row["date"], row["link"], row["summary"], row["fetched_at"]))
    if cur.rowcount == 1:
        # article inséré à l'instant
        return cur.lastrowid
    # déjà présent : on récupère l'id existant via le link (UNIQUE)
    r = con.execute("SELECT id FROM articles WHERE link = ?", (row["link"],)).fetchone()
    return r[0] if r else None

def choose_nlp(text: str):
    """
    Choisit le meilleur pipeline dispo (FR ou EN). Si les deux sont chargés,
    garde celui qui renvoie le plus d'entités. Fallback: aucun (retourne None).
    """
    docs = []
    if nlp_fr:
        docs.append(nlp_fr(text))
    if nlp_en:
        docs.append(nlp_en(text))
    if not docs:
        return None
    # garde le doc avec le plus d'entités
    return max(docs, key=lambda d: len(d.ents))

def store_ner(con, article_id: int, text: str):
    """
    Lance spaCy sur `text` et insère les entités dans la table `entities`.
    """
    if not article_id or not text:
        return
    doc = choose_nlp(text)
    if not doc or not doc.ents:
        return
    rows = [(article_id, ent.text, ent.label_, ent.start_char, ent.end_char) for ent in doc.ents]
    con.executemany("""
        INSERT OR IGNORE INTO entities (article_id, text, label, start, "end")
        VALUES (?, ?, ?, ?, ?)
    """, rows)



def main():
    ensure_db()
    total_new = 0
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
                }
                if not row["link"]:
                    continue

                # --- insertion article + récupération id ---
                before = con.total_changes
                article_id = insert_article_return_id(con, row)
                if con.total_changes > before:
                    added += 1  # nouvel article

                # --- NER + enrichissement ---
                text_for_ner = f"{row['title']} {row['summary']}".strip()
                doc, lang = choose_nlp_doc(text_for_ner, row["link"])

                if doc and doc.ents:
                    rows_ner = [(article_id, ent.text, ent.label_, ent.start_char, ent.end_char) for ent in doc.ents]
                    con.executemany("""
                        INSERT OR IGNORE INTO entities (article_id, text, label, start, "end")
                        VALUES (?,?,?,?,?)
                    """, rows_ner)

                # métadonnées éditeur + langue
                update_article_publisher(con, article_id, row["link"], lang)

                # topics (sur le même texte)
                store_topics(con, article_id, text_for_ner)


            total_new += added
            print(f"+{added} nouveaux depuis ce flux\n")

    print(f"Terminé. {total_new} nouveaux articles insérés dans {DB_PATH}.")


if __name__ == "__main__":
    main()

