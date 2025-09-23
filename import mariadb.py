import mariadb
import pandas as pd
from gdeltdoc import GdeltDoc, Filters

# --- 1. Config connexion MariaDB ---
DB_CONFIG = {
    "host": "localhost",
    "user": "florent",           
    "password": "2003",  
    "database": "gdelt_db"
}
