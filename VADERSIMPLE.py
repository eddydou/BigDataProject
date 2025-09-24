# pip install gdeltdoc vaderSentiment beautifulsoup4 sqlalchemy pymysql

import pandas as pd
import re, html
from bs4 import BeautifulSoup
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from gdeltdoc import GdeltDoc, Filters, repeat
import mariadb  



# ==== DB SETUP ===============================================================
USE_MARIADB = True 

from sqlalchemy import create_engine, text

USER = "root"
PWD  = "2003"
HOST = "127.0.0.1"
PORT = 3306
DB   = "NewsVader"     

if USE_MARIADB:
    ENGINE_URL = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{DB}?charset=utf8mb4"
else:
    ENGINE_URL = "sqlite:///NewsVader.db"

if USE_MARIADB:
    ADMIN_URL = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/?charset=utf8mb4"
    admin_engine = create_engine(ADMIN_URL, future=True, pool_pre_ping=True)
    with admin_engine.begin() as conn:
        conn.exec_driver_sql(f"""
            CREATE DATABASE IF NOT EXISTS {DB}
            CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
        """)

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
  seendate VARCHAR(32) NULL,
  published_at VARCHAR(32) NULL,
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

print("Connexion et schéma OK :", ENGINE_URL)

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
from gdeltdoc import GdeltDoc, Filters, repeat
# ==== GDELT -> DF avec Multiple Batches ====================================
def get_multiple_batches(num_batches=6):  # 6 × 250 = 1500 articles
    gd = GdeltDoc()
    all_articles = []
    
    # Diviser la période en 6 parties
    from datetime import datetime, timedelta
    import time
    
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 9, 20)
    total_days = (end_date - start_date).days
    days_per_batch = total_days // num_batches
    
    current_date = start_date
    
    for i in range(num_batches):
        if i == num_batches - 1:
            period_end = end_date
        else:
            period_end = current_date + timedelta(days=days_per_batch)
        
        f = Filters(
            start_date=current_date.strftime("%Y-%m-%d"),
            end_date=period_end.strftime("%Y-%m-%d"),
            num_records=250,  # Maximum autorisé
            language="ENGLISH",
            domain=["bbc.co.uk", "bloomberg.com", "theguardian.com", "ft.com","economist.com"]
        )
        
        try:
            df_batch = gd.article_search(f)
            if not df_batch.empty:
                all_articles.append(df_batch)
                print(f"Batch {i+1} ({current_date.strftime('%Y-%m-%d')} à {period_end.strftime('%Y-%m-%d')}): {len(df_batch)} articles")
        except Exception as e:
            print(f"Erreur batch {i+1}: {e}")
        
        current_date = period_end
        time.sleep(1)  # Pause entre requêtes
    
    if all_articles:
        final_df = pd.concat(all_articles, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['url'], keep='first')
        print(f"Total final après suppression doublons: {len(final_df)} articles")
        return final_df
    return pd.DataFrame()

# Récupération des articles
df = get_multiple_batches(6)  # Pour ~1500 articles

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
        # Dates : aucune transformation, on force juste en string pour l'insert
        "seendate": seendt,
        "published_at": pubdt
    })


    out["sentiment_compound"] = scores.map(lambda s: s["compound"])
    out["sentiment_pos"]      = scores.map(lambda s: s["pos"])
    out["sentiment_neu"]      = scores.map(lambda s: s["neu"])
    out["sentiment_neg"]      = scores.map(lambda s: s["neg"])
    out["sentiment_label"]    = out["sentiment_compound"].map(label_from_compound)

    print("🔍 COLONNES DATES ORIGINALES:")
    if not seendt.empty and seendt.notna().any():
        print(f"seendate exemples: {seendt.dropna().head(3).tolist()}")
    if not pubdt.empty and pubdt.notna().any():
        print(f"published_at exemples: {pubdt.dropna().head(3).tolist()}")

    print(f"Résultat final - seendate nulles: {out['seendate'].isna().sum()}/{len(out)}")
    print(f"Résultat final - published_at nulles: {out['published_at'].isna().sum()}/{len(out)}")
    
    return out

scored = sentiment_on_df(df)

# ==== UPSERT EN DB ===========================================================
from datetime import datetime, date

def _to_sql_value_dt(x):
    # Laisse passer tel quel, sauf None/NaN
    if x is None:
        return None
    try:
        import pandas as pd
        if pd.isna(x):
            return None
    except Exception:
        pass
    return str(x)  # aucune modification de format


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
