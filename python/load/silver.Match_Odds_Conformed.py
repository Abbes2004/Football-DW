import pyodbc
import pandas as pd
from datetime import datetime
import numpy as np
from io import StringIO

# --- 1. CONFIGURATION ---
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'

# Tables
BRONZE_TABLE = 'bronze.staging_epl_matchs'
SILVER_DESTINATION_TABLE = 'silver.Match_Odds_Conformed'
TEAM_MAPPING_TABLE = 'silver.Team_Mapping' 

CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# --- 2. FONCTIONS DE TRANSFORMATION ---

def derive_season(match_date):
    """
    Dérive la saison (AAAA/AA) à partir de la date du match.
    Ex: 2015-08-08 -> 2015/16
    """
    if pd.isnull(match_date):
        return None
    
    match_date = pd.to_datetime(match_date)
    year = match_date.year
    month = match_date.month
    
    if month >= 8:
        # Match en 2015-08 -> Saison 2015/16
        start_year = year
        end_year_abbreviated = (year + 1) % 100
    else:
        # Match en 2016-04 -> Saison 2015/16
        start_year = year - 1
        end_year_abbreviated = year % 100
    
    # Formatage AAAA/AA, en s'assurant que l'année abrégée a deux chiffres
    return f'{start_year}/{end_year_abbreviated:02d}'


def run_etl():
    """Exécute l'intégralité du processus ETL."""
    conn = None
    try:
        conn = pyodbc.connect(CONN_STRING, autocommit=True)
        print("Connexion à la base de données établie.")
        
        # --- E: EXTRACTION ---
        
        sql_bronze = f"SELECT * FROM {BRONZE_TABLE};"
        df_bronze = pd.read_sql(sql_bronze, conn)
        
        sql_mapping = f"SELECT Team_Source_Name, Team_Standard_Name FROM {TEAM_MAPPING_TABLE};"
        df_mapping = pd.read_sql(sql_mapping, conn)
        
        team_map = df_mapping.set_index('Team_Source_Name')['Team_Standard_Name'].to_dict()
        
        print(f"Extraction terminée. {len(df_bronze)} lignes extraites du Bronze.")
        
        # --- T: TRANSFORMATION ---

        df_silver = df_bronze.copy()
        
        # A. Dérivation et Typage de la Date (Gestion des multiples formats)
        
        df_silver['Date_str'] = df_silver['Date'].astype(str).str.strip()
        
        # 1. Première tentative : Format DD/MM/AAAA (ex: 24/08/2015)
        df_silver['Date_converted'] = pd.to_datetime(
            df_silver['Date_str'], format='%d/%m/%Y', errors='coerce'
        )
        
        # 2. Deuxième tentative : Format AAAA-MM-JJ (format par défaut) pour les lignes qui ont échoué
        mask_nat = df_silver['Date_converted'].isna()
        
        df_silver.loc[mask_nat, 'Date_converted'] = pd.to_datetime(
            df_silver.loc[mask_nat, 'Date_str'], errors='coerce'
        )

        # 3. Gestion des NaT persistants (dates totalement invalides)
        rows_with_invalid_date = df_silver['Date_converted'].isna().sum()
        
        if rows_with_invalid_date > 0:
            print(f"⚠️ Avertissement : {rows_with_invalid_date} lignes avec des dates non convertibles détectées et exclues.")
            df_silver.dropna(subset=['Date_converted'], inplace=True)
            
        # 4. Finalisation
        df_silver['MatchDate'] = df_silver['Date_converted'].dt.date
        df_silver.drop(columns=['Date', 'Date_str', 'Date_converted'], inplace=True)

        
        # B. Dérivation de la Saison (Utilise la fonction corrigée)
        df_silver['Season'] = df_silver['MatchDate'].apply(derive_season)
        
        # C. Standardisation des Noms d'Équipes
        df_silver['HomeTeam_Conformed'] = df_silver['HomeTeam'].map(team_map).fillna(df_silver['HomeTeam'])
        df_silver['AwayTeam_Conformed'] = df_silver['AwayTeam'].map(team_map).fillna(df_silver['AwayTeam'])
        
        # D. Renommage et Conversion des Types
        
        int_cols_map = {
            'FTHG': 'FullTimeHomeGoals', 'FTAG': 'FullTimeAwayGoals', 
            'HTHG': 'HalfTimeHomeGoals', 'HTAG': 'HalfTimeAwayGoals', 
            'HS': 'HomeShots', 'AS': 'AwayShots', 
            'HST': 'HomeShotsOnTarget', 'AST': 'AwayShotsOnTarget', 
            'HF': 'HomeFouls', 'AF': 'AwayFouls', 
            'HC': 'HomeCorners', 'AC': 'AwayCorners',
            'HY': 'HomeYellowCards', 'AY': 'AwayYellowCards',
            'HR': 'HomeRedCards', 'AR': 'AwayRedCards'
        }
        
        for old, new in int_cols_map.items():
            df_silver[new] = pd.to_numeric(df_silver[old], errors='coerce').fillna(0).astype(int)
            
        str_dec_map = {
            'FTR': 'FullTimeResult', 'HTR': 'HalfTimeResult',
            'B365H': 'B365HomeOdds', 'B365D': 'B365DrawOdds', 'B365A': 'B365AwayOdds'
        }
        for old, new in str_dec_map.items():
            df_silver[new] = df_silver[old]
            
        # E. Sélection des colonnes finales
        final_cols = [
            'MatchDate', 'Season', 'HomeTeam_Conformed', 'AwayTeam_Conformed',
            'FullTimeHomeGoals', 'FullTimeAwayGoals', 'FullTimeResult',
            'HalfTimeHomeGoals', 'HalfTimeAwayGoals', 'HalfTimeResult',
            'HomeShots', 'AwayShots', 'HomeShotsOnTarget', 'AwayShotsOnTarget',
            'HomeFouls', 'AwayFouls', 'HomeCorners', 'AwayCorners',
            'HomeYellowCards', 'AwayYellowCards', 'HomeRedCards', 'AwayRedCards',
            'B365HomeOdds', 'B365DrawOdds', 'B365AwayOdds'
        ]
        
        df_final = df_silver[final_cols]
        print(f"Transformation terminée. {len(df_final)} lignes prêtes pour le chargement.")
        
        # --- L: CHARGEMENT ---
        
        cursor = conn.cursor()
        print(f"Suppression des données existantes dans {SILVER_DESTINATION_TABLE}...")
        cursor.execute(f"TRUNCATE TABLE {SILVER_DESTINATION_TABLE};")
        conn.commit()
        
        placeholders = ', '.join(['?' for _ in final_cols])
        insert_sql = f"INSERT INTO {SILVER_DESTINATION_TABLE} VALUES ({placeholders})"
        
        data_to_insert = [tuple(row) for row in df_final.replace({np.nan: None, pd.NaT: None}).values]

        print(f"Début du chargement de {len(data_to_insert)} lignes...")
        
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"🎉 ETL terminé avec succès. {len(data_to_insert)} lignes insérées dans {SILVER_DESTINATION_TABLE}.")
        
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"❌ Erreur lors de l'exécution de l'ETL : {sqlstate}")
        print(ex)
    except Exception as e:
        print(f"❌ Erreur Critique inattendue : {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    run_etl()