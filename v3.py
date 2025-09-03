# rss_to_db.py
import feedparser, requests, sqlite3, os
from datetime import datetime, UTC

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

def ensure_db():
    with sqlite3.connect(DB_PATH) as con:
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
        # index utile sur date+source
        con.execute("CREATE INDEX IF NOT EXISTS idx_articles_date_source ON articles(date, source);")


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
                    continue  # on skippe si pas de lien
                insert_article(con, row)
                # sqlite te dit combien de lignes ont changé via changes()
                if con.total_changes > total_new + added:
                    added += 1

            total_new += added
            print(f"+{added} nouveaux depuis ce flux\n")

    print(f"Terminé. {total_new} nouveaux articles insérés dans {DB_PATH}.")

if __name__ == "__main__":
    main()

