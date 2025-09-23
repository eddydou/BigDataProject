# pip install gdeltdoc vaderSentiment beautifulsoup4 sqlalchemy pymysql

import pandas as pd
import re, html
from bs4 import BeautifulSoup
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from gdeltdoc import GdeltDoc, Filters, repeat


# ==== DB SETUP ===============================================================
USE_MARIADB = True  # <-- passe à True si tu veux MariaDB

from sqlalchemy import create_engine, text
if USE_MARIADB:
    # Crée la base NewsVader si absente (il faut les droits côté serveur)
    ENGINE_URL = "mysql+pymysql://newsuser:strongpwd@127.0.0.1:3306/NewsVader?charset=utf8mb4"
else:
    # SQLite fichier local (auto-crée NewsVader.db)
    ENGINE_URL = "sqlite:///NewsVader.db"

engine = create_engine(ENGINE_URL, future=True, pool_pre_ping=True)

DDL_ARTICLES_SQLITE = """
CREATE TABLE IF NOT EXISTS articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT,
  url TEXT NOT NULL,
  title TEXT,
  description TEXT,
  content TEXT,
  full_text TEXT,
  seendate TEXT,
  published_at TEXT,
  language TEXT,
  sentiment_compound REAL,
  sentiment_pos REAL,
  sentiment_neu REAL,
  sentiment_neg REAL,
  sentiment_label TEXT,
  UNIQUE(url) ON CONFLICT IGNORE
);
"""

DDL_ARTICLES_MYSQL = """
CREATE TABLE IF NOT EXISTS articles (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  source VARCHAR(255),
  url VARCHAR(1024) NOT NULL,
  title VARCHAR(1024),
  description TEXT,
  content MEDIUMTEXT,
  full_text MEDIUMTEXT,
  seendate DATETIME NULL,
  published_at DATETIME NULL,
  language VARCHAR(16),
  sentiment_compound DOUBLE,
  sentiment_pos DOUBLE,
  sentiment_neu DOUBLE,
  sentiment_neg DOUBLE,
  sentiment_label VARCHAR(16),
  UNIQUE KEY uk_url (url),
  KEY idx_seendate (seendate),
  KEY idx_published (published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

with engine.begin() as conn:
    conn.exec_driver_sql(DDL_ARTICLES_MYSQL if USE_MARIADB else DDL_ARTICLES_SQLITE)

# ==== VADER + CLEAN ==========================================================
analyzer = SentimentIntensityAnalyzer()

def clean_text_soft(text: str) -> str:
    """Nettoyage doux: enlève HTML/URLs, garde chiffres, % et devises."""
    if not isinstance(text, str):
        return ""
    text = html.unescape(text)
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    text = re.sub(r'(https?://\S+|www\.\S+)', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def label_from_compound(x: float) -> str:
    return "Positive" if x >= 0.05 else ("Negative" if x <= -0.05 else "Neutral")

# ==== GDELT -> DF ============================================================
f = Filters(
    start_date="2024-01-01",
    end_date="2025-09-20",
    num_records=250,
    language="ENGLISH", 
    #domain=["bbc.co.uk", "bloomberg.com", 
    #       "lemonde.fr", "lesechos.fr"],  
    repeat=repeat(4,"Political")           
)
gd = GdeltDoc()
df = gd.article_search(f)  # DataFrame GDELT

# ==== SCORING ================================================================
def sentiment_on_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Colonnes tolérantes (GDELT varie selon endpoints)
    title   = df["title"]        if "title" in df.columns else pd.Series([""]*len(df))
    content = df["content"]      if "content" in df.columns else pd.Series([""]*len(df))
    desc    = df["description"]  if "description" in df.columns else pd.Series([""]*len(df))
    snip    = df["snippet"]      if "snippet" in df.columns else pd.Series([""]*len(df))
    lang    = df["language"]     if "language" in df.columns else pd.Series([""]*len(df))
    url     = df["url"]          if "url" in df.columns else df.get("DocumentIdentifier", pd.Series([""]*len(df)))
    source  = df["domain"]       if "domain" in df.columns else df.get("sourceCommonName", pd.Series([""]*len(df)))
    seendt  = df["seendate"]     if "seendate" in df.columns else pd.Series([None]*len(df))
    pubdt   = df["publishdate"]  if "publishdate" in df.columns else df.get("date", pd.Series([None]*len(df)))

    full_text = (title.fillna("") + " " + content.fillna("") + " " +
                 desc.fillna("") + " " + snip.fillna("")).map(clean_text_soft)

    scores = full_text.map(lambda t: analyzer.polarity_scores(t) if t else {"compound":0,"pos":0,"neu":1,"neg":0})

    out = pd.DataFrame({
        "source":  source.astype(str).str[:255],
        "url":     url.astype(str).str[:1024],
        "title":   title.astype(str),
        "description": desc.astype(str),
        "content": content.astype(str),
        "full_text": full_text,
        "language": lang.astype(str).str[:16],
        "seendate": seendt,
        "published_at": pubdt
    })

    out["sentiment_compound"] = scores.map(lambda s: s["compound"])
    out["sentiment_pos"]      = scores.map(lambda s: s["pos"])
    out["sentiment_neu"]      = scores.map(lambda s: s["neu"])
    out["sentiment_neg"]      = scores.map(lambda s: s["neg"])
    out["sentiment_label"]    = out["sentiment_compound"].map(label_from_compound)

    # Parse dates => formats DB
    def to_dt(x):
        # GDELT peut donner '20240921T123000Z' ou '2024-09-21' / '20240921123000'
        if pd.isna(x) or x is None: return None
        s = str(x).replace("Z","")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d%H%M%S", "%Y%m%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt)], fmt)
            except Exception:
                continue
        return None

    out["seendate"]     = out["seendate"].map(to_dt)
    out["published_at"] = out["published_at"].map(to_dt)
    return out

scored = sentiment_on_df(df)

# ==== UPSERT EN DB ===========================================================
from datetime import datetime, date

def _to_sql_value_dt(x):
    """Convertit pd.Timestamp/NaT en str ISO (ou None) pour SQLite."""
    if x is None:
        return None
    # pandas NaT -> None
    try:
        import pandas as pd
        if isinstance(x, pd._libs.tslibs.nattype.NaTType):
            return None
        if isinstance(x, pd.Timestamp):
            x = x.to_pydatetime()
    except Exception:
        pass
    if isinstance(x, (datetime, date)):
        return x.strftime("%Y-%m-%d %H:%M:%S")
    # parfois c'est déjà une str ISO/yyyymmdd etc.
    s = str(x).strip()
    return s if s else None

def upsert_articles(df_scored: pd.DataFrame):
    if df_scored.empty:
        print("Aucun article à insérer.")
        return 0, 0

    # sécurise les colonnes qui partent dans la DB
    cols = ["source","url","title","description","content","full_text",
            "seendate","published_at","language",
            "sentiment_compound","sentiment_pos","sentiment_neu","sentiment_neg","sentiment_label"]

    # si certaines colonnes manquent, crée-les vides
    for c in cols:
        if c not in df_scored.columns:
            df_scored[c] = None

    # convertit les dates en str ISO pour SQLite
    payload = []
    for _, r in df_scored.iterrows():
        rec = {c: r.get(c) for c in cols}
        rec["seendate"]     = _to_sql_value_dt(rec["seendate"])
        rec["published_at"] = _to_sql_value_dt(rec["published_at"])
        payload.append(rec)

    with engine.begin() as conn:
        if USE_MARIADB:
            sql = text("""
            INSERT INTO articles
              (source, url, title, description, content, full_text,
               seendate, published_at, language,
               sentiment_compound, sentiment_pos, sentiment_neu, sentiment_neg, sentiment_label)
            VALUES
              (:source, :url, :title, :description, :content, :full_text,
               :seendate, :published_at, :language,
               :sentiment_compound, :sentiment_pos, :sentiment_neu, :sentiment_neg, :sentiment_label)
            ON DUPLICATE KEY UPDATE
              title=VALUES(title),
              description=VALUES(description),
              content=VALUES(content),
              full_text=VALUES(full_text),
              seendate=VALUES(seendate),
              published_at=VALUES(published_at),
              language=VALUES(language),
              sentiment_compound=VALUES(sentiment_compound),
              sentiment_pos=VALUES(sentiment_pos),
              sentiment_neu=VALUES(sentiment_neu),
              sentiment_neg=VALUES(sentiment_neg),
              sentiment_label=VALUES(sentiment_label);
            """)
        else:
            sql = text("""
            INSERT INTO articles
              (source, url, title, description, content, full_text,
               seendate, published_at, language,
               sentiment_compound, sentiment_pos, sentiment_neu, sentiment_neg, sentiment_label)
            VALUES
              (:source, :url, :title, :description, :content, :full_text,
               :seendate, :published_at, :language,
               :sentiment_compound, :sentiment_pos, :sentiment_neu, :sentiment_neg, :sentiment_label)
            ON CONFLICT(url) DO UPDATE SET
              title=excluded.title,
              description=excluded.description,
              content=excluded.content,
              full_text=excluded.full_text,
              seendate=excluded.seendate,
              published_at=excluded.published_at,
              language=excluded.language,
              sentiment_compound=excluded.sentiment_compound,
              sentiment_pos=excluded.sentiment_pos,
              sentiment_neu=excluded.sentiment_neu,
              sentiment_neg=excluded.sentiment_neg,
              sentiment_label=excluded.sentiment_label;
            """)

        conn.execute(sql, payload)

    print(f"Écrit dans la base: {len(payload)} lignes (insert+update confondus).")
    return len(payload), 0


if not scored.empty:
    # (optionnel) un aperçu avant insertion
    print(scored[["title","sentiment_label","sentiment_compound"]].head(10))

    upsert_articles(scored)
    print("OK. Base prête :", ENGINE_URL)
else:
    print("Aucun article renvoyé par GDELT.")
