# pip install gdeltdoc vaderSentiment beautifulsoup4 sqlalchemy pymysql
import pandas as pd
import re, html
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from gdeltdoc import GdeltDoc, Filters, repeat

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

# Crée la base si elle n'existe pas (MariaDB)
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
  published_date TEXT,
  gdelt_date TEXT,
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
CREATE TABLE IF NOT EXISTS `articles` (
  `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `source` VARCHAR(255),
  `url` TEXT NOT NULL,
  `url_hash` CHAR(32) NOT NULL,
  `title` TEXT,
  `description` MEDIUMTEXT,
  `content` MEDIUMTEXT,
  `full_text` MEDIUMTEXT,
  `published_date` VARCHAR(32) NULL,    -- Date de publication de l'article
  `gdelt_date` VARCHAR(32) NULL,        -- Date de découverte par GDELT
  `language` VARCHAR(16),
  `sentiment_compound` DOUBLE,
  `sentiment_pos` DOUBLE,
  `sentiment_neu` DOUBLE,
  `sentiment_neg` DOUBLE,
  `sentiment_label` VARCHAR(16),
  UNIQUE KEY `uk_url_hash` (`url_hash`),
  KEY `idx_published_date` (`published_date`),
  KEY `idx_gdelt_date` (`gdelt_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
"""

def create_table_safely():
    """Crée la table articles de manière sécurisée avec vérifications"""
    try:
        with engine.begin() as conn:
            # Vérification si la table existe déjà
            if USE_MARIADB:
                result = conn.exec_driver_sql("""
                    SELECT COUNT(*) as count FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = 'articles'
                """, (DB,))
                exists = result.scalar() > 0
            else:
                result = conn.exec_driver_sql("""
                    SELECT COUNT(*) as count FROM sqlite_master 
                    WHERE type='table' AND name='articles'
                """)
                exists = result.scalar() > 0
            
            if exists:
                print("✅ Table 'articles' existe déjà")
            else:
                print("📝 Création de la table 'articles'...")
                conn.exec_driver_sql(DDL_ARTICLES_MYSQL if USE_MARIADB else DDL_ARTICLES_SQLITE)
                print("✅ Table 'articles' créée avec succès")
            
            # Vérification finale
            if USE_MARIADB:
                result = conn.exec_driver_sql("SHOW TABLES LIKE 'articles'")
                if result.rowcount == 0:
                    raise Exception("La table 'articles' n'a pas été créée correctement")
            else:
                result = conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='articles'")
                if not result.fetchone():
                    raise Exception("La table 'articles' n'a pas été créée correctement")
                    
        print("Connexion et schéma OK :", ENGINE_URL)
        
    except Exception as e:
        print(f"❌ Erreur lors de la création de la table: {e}")
        print("Tentative de création forcée...")
        
        # Tentative de création forcée
        try:
            with engine.begin() as conn:
                if USE_MARIADB:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS articles")
                conn.exec_driver_sql(DDL_ARTICLES_MYSQL if USE_MARIADB else DDL_ARTICLES_SQLITE)
            print("✅ Table créée après suppression forcée")
        except Exception as e2:
            print(f"❌ Impossible de créer la table: {e2}")
            raise

# Création de la table
create_table_safely()

# ==== VADER + CLEAN ==========================================================
analyzer = SentimentIntensityAnalyzer()

def clean_text_soft(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = html.unescape(text)
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    text = re.sub(r'(https?://\S+|www\.\S+)', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def label_from_compound(x: float) -> str:
    return "Positive" if x >= 0.05 else ("Negative" if x <= -0.05 else "Neutral")

def generate_url_hash(url: str) -> str:
    """Génère un hash MD5 de l'URL pour la déduplication"""
    if not url:
        return ""
    return hashlib.md5(url.encode('utf-8')).hexdigest()

# ==== GDELT -> DF avec Multiple Batches ====================================
def get_multiple_batches(num_batches=6):
    gd = GdeltDoc()
    all_articles = []
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
            num_records=250,
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
        time.sleep(1)
    
    if all_articles:
        final_df = pd.concat(all_articles, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['url'], keep='first')
        print(f"Total final après suppression doublons: {len(final_df)} articles")
        return final_df
    return pd.DataFrame()

# ==== SCORING ================================================================
def sentiment_on_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
        
    title   = df["title"]        if "title" in df.columns else pd.Series([""]*len(df))
    content = df["content"]      if "content" in df.columns else pd.Series([""]*len(df))
    desc    = df["description"]  if "description" in df.columns else pd.Series([""]*len(df))
    snip    = df["snippet"]      if "snippet" in df.columns else pd.Series([""]*len(df))
    lang    = df["language"]     if "language" in df.columns else pd.Series([""]*len(df))
    url     = df["url"]          if "url" in df.columns else df.get("DocumentIdentifier", pd.Series([""]*len(df)))
    source  = df["domain"]       if "domain" in df.columns else df.get("sourceCommonName", pd.Series([""]*len(df)))
    
    # Gestion séparée des deux types de dates
    published_date = None
    gdelt_date = None
    
    # Date de publication de l'article (priorité : publishdate > date)
    if "publishdate" in df.columns:
        published_date = df["publishdate"]
    elif "date" in df.columns:
        published_date = df["date"]
    else:
        published_date = pd.Series([None]*len(df))
    
    # Date de découverte par GDELT
    if "seendate" in df.columns:
        gdelt_date = df["seendate"]
    else:
        gdelt_date = pd.Series([None]*len(df))
    
    full_text = (title.fillna("") + " " + content.fillna("") + " " + desc.fillna("") + " " + snip.fillna("")).map(clean_text_soft)
    scores = full_text.map(lambda t: analyzer.polarity_scores(t) if t else {"compound":0,"pos":0,"neu":1,"neg":0})
    
    out = pd.DataFrame({
        "source":  source.astype(str).str[:255],
        "url":     url.astype(str).str[:1024],
        "url_hash": url.astype(str).map(generate_url_hash),
        "title":   title.astype(str),
        "description": desc.astype(str),
        "content": content.astype(str),
        "full_text": full_text,
        "language": lang.astype(str).str[:16],
        "published_date": published_date,  # Date de publication
        "gdelt_date": gdelt_date          # Date de découverte GDELT
    })
    
    out["sentiment_compound"] = scores.map(lambda s: s["compound"])
    out["sentiment_pos"]      = scores.map(lambda s: s["pos"])
    out["sentiment_neu"]      = scores.map(lambda s: s["neu"])
    out["sentiment_neg"]      = scores.map(lambda s: s["neg"])
    out["sentiment_label"]    = out["sentiment_compound"].map(label_from_compound)
    
    print("🔍 COLONNES DATES ORIGINALES:")
    if published_date is not None and not published_date.empty and published_date.notna().any():
        print(f"published_date exemples: {published_date.dropna().head(3).tolist()}")
    if gdelt_date is not None and not gdelt_date.empty and gdelt_date.notna().any():
        print(f"gdelt_date exemples: {gdelt_date.dropna().head(3).tolist()}")
    
    print(f"Résultat final - published_date nulles: {out['published_date'].isna().sum()}/{len(out)}")
    print(f"Résultat final - gdelt_date nulles: {out['gdelt_date'].isna().sum()}/{len(out)}")
    
    return out

# ==== FONCTION DE NETTOYAGE DES DATES ======================================
def _to_sql_value_dt(x):
    """Convertit une valeur de date en string pour stockage SQL"""
    if x is None:
        return None
    try:
        import pandas as pd
        if pd.isna(x):
            return None
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip()
        if s == "" or s.lower() in ("none", "nan", "nat", "null"):
            return None
        return s
    return str(x)

# ==== UPSERT EN DB ===========================================================
def upsert_articles(df_scored: pd.DataFrame):
    if df_scored.empty:
        print("Aucun article à insérer.")
        return 0, 0

    # Vérification que la table existe avant insertion
    try:
        with engine.begin() as conn:
            if USE_MARIADB:
                result = conn.exec_driver_sql("SHOW TABLES LIKE 'articles'")
                if result.rowcount == 0:
                    raise Exception("Table 'articles' non trouvée")
            else:
                result = conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='articles'")
                if not result.fetchone():
                    raise Exception("Table 'articles' non trouvée")
    except Exception as e:
        print(f"❌ Erreur: {e}")
        print("Recréation de la table...")
        create_table_safely()

    if USE_MARIADB:
        cols = ["source","url","url_hash","title","description","content","full_text",
                "published_date","gdelt_date","language",
                "sentiment_compound","sentiment_pos","sentiment_neu","sentiment_neg","sentiment_label"]
    else:
        cols = ["source","url","title","description","content","full_text",
                "published_date","gdelt_date","language",
                "sentiment_compound","sentiment_pos","sentiment_neu","sentiment_neg","sentiment_label"]

    for c in cols:
        if c not in df_scored.columns:
            df_scored[c] = None

    payload = []
    for _, r in df_scored.iterrows():
        rec = {c: r.get(c) for c in cols}
        rec["published_date"] = _to_sql_value_dt(rec["published_date"])
        rec["gdelt_date"] = _to_sql_value_dt(rec["gdelt_date"])
        payload.append(rec)

    try:
        with engine.begin() as conn:
            if USE_MARIADB:
                sql = text("""
                INSERT INTO articles
                  (source, url, url_hash, title, description, content, full_text,
                   gdelt_date, language,
                   sentiment_compound, sentiment_pos, sentiment_neu, sentiment_neg, sentiment_label)
                VALUES
                  (:source, :url, :url_hash, :title, :description, :content, :full_text,
                   :gdelt_date, :language,
                   :sentiment_compound, :sentiment_pos, :sentiment_neu, :sentiment_neg, :sentiment_label)
                ON DUPLICATE KEY UPDATE
                  title=VALUES(title),
                  description=VALUES(description),
                  content=VALUES(content),
                  full_text=VALUES(full_text),
                  gdelt_date=VALUES(gdelt_date),
                  language=VALUES(language),
                  sentiment_compound=VALUES(sentiment_compound),
                  sentiment_pos=VALUES(sentiment_pos),
                  sentiment_neu=VALUES(sentiment_neu),
                  sentiment_neg=VALUES(sentiment_neg),
                  sentiment_label=VALUES(sentiment_label)
                """)
            else:
                sql = text("""
                INSERT INTO articles
                  (source, url, title, description, content, full_text,
                   gdelt_date, language,
                   sentiment_compound, sentiment_pos, sentiment_neu, sentiment_neg, sentiment_label)
                VALUES
                  (:source, :url, :title, :description, :content, :full_text,
                   :gdelt_date, :language,
                   :sentiment_compound, :sentiment_pos, :sentiment_neu, :sentiment_neg, :sentiment_label)
                ON CONFLICT(url) DO UPDATE SET
                  title=excluded.title,
                  description=excluded.description,
                  content=excluded.content,
                  full_text=excluded.full_text,
                  gdelt_date=excluded.gdelt_date,
                  language=excluded.language,
                  sentiment_compound=excluded.sentiment_compound,
                  sentiment_pos=excluded.sentiment_pos,
                  sentiment_neu=excluded.sentiment_neu,
                  sentiment_neg=excluded.sentiment_neg,
                  sentiment_label=excluded.sentiment_label
                """)

            conn.execute(sql, payload)

        print(f"✅ Écrit dans la base: {len(payload)} lignes (insert+update confondus).")
        return len(payload), 0
        
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion: {e}")
        
        # En cas d'erreur, afficher quelques exemples de données pour debug
        print("🔍 Exemples de données à insérer:")
        for i, item in enumerate(payload[:3]):
            print(f"  Ligne {i+1}: {list(item.keys())}")
        raise

# ==== EXÉCUTION ==============================================================
print("Récupération des articles GDELT...")
df = get_multiple_batches(6)

if not df.empty:
    print("Analyse de sentiment...")
    scored = sentiment_on_df(df)
    
    if not scored.empty:
        print("\nAperçu des résultats:")
        print(scored[["title","sentiment_label","sentiment_compound"]].head(10))
        
        print("\nInsertion en base de données...")
        upsert_articles(scored)
        print("✅ Terminé avec succès :", ENGINE_URL)
    else:
        print("Aucun article après scoring.")
else:
    print("Aucun article récupéré depuis GDELT.")