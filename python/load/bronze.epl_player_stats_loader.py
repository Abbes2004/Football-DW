import pandas as pd
import pyodbc
import os
import re

# --- 1. CONFIGURATION DU PROJET ---
# Chemin du dossier source pour les fichiers JSON 'Player Stats'
JSON_DIRECTORY = r'D:\Abbes\Football-DW-Project\data\processed\epl_player_stats_json'
JSON_PATTERN = '.json'

# Informations de Connexion SQL Server (Sauvegardées)
SQL_SERVER_NAME = '(localdb)\MSSQLLocalDB'
DATABASE_NAME = 'DW_Football_Staging'
STAGING_TABLE = 'bronze.staging_player_stats' 

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# --- 2. Fonction de Génération de Saison (CORRIGÉE) ---
def generate_season_from_filename(filename):
    """
    Extrait l'année de la saison du nom de fichier (ex: '2014-2015_player_info.json' -> '2014/15').
    """
    # Regex pour trouver le motif de début 'YYYY-YYYY'
    match = re.search(r'^(\d{4})\-(\d{4})\_', filename)
    if match:
        start_year = match.group(1)
        end_year = match.group(2)
        
        # Le format DDL est 'YYYY/YY'
        return f"{start_year}/{end_year[2:]}" # Ex: 2014/15
    
    return "Unknown/Season" 

# --- 3. FONCTION PRINCIPALE ETL ---
def extract_transform_load_player_stats():
    """
    Parcourt tous les fichiers JSON, les transforme et les charge dans la table PLAYER STATS de staging.
    """
    total_rows_loaded = 0
    
    # 3.1. Connexion à la base de données
    try:
        conn = pyodbc.connect(CONN_STRING)
        cursor = conn.cursor()
        print(f"Connexion à SQL Server établie sur [{DATABASE_NAME}].")
    except Exception as e:
        print(f"Échec de la connexion à SQL Server. Vérifiez le serveur et le pilote : {e}")
        return

    # 3.2. La Boucle "For Each File"
    for filename in os.listdir(JSON_DIRECTORY):
        if filename.endswith(JSON_PATTERN):
            file_path = os.path.join(JSON_DIRECTORY, filename)
            season = generate_season_from_filename(filename) # <--- Utilise la fonction corrigée
            print(f"\n--- Traitement du fichier : {filename} (Saison: {season}) ---")

            try:
                # --- EXTRACTION (E) ---
                df = pd.read_json(file_path)
                
                # --- TRANSFORMATION (T) ---
                
                # a) Créer la colonne Season (Manquante dans le JSON)
                df.insert(0, 'Season', season)
                
                # b) Renommage des colonnes
                df = df.rename(columns={
                    '90s': '90s_col', # Renommage temporaire pour le DataFrame
                    'market_value_€k': 'market_value_euro_k'
                    # Pas de changement pour Gls_1 et Ast_1, les noms sont corrects
                })
                
                # c) Nettoyage et Conversion des types
                
                # Nettoyage de la colonne 'Min' (contient des virgules '2,307')
                df['Min'] = df['Min'].astype(str).str.replace(',', '').str.strip()
                
                # Colonnes INT
                int_cols = ['Rk', 'Age', 'Born', 'MP', 'Starts', 'Min', 'Gls', 'Ast', 'CrdY', 'CrdR', 'market_value_euro_k']
                for col in int_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

                # Colonnes DECIMAL(5,2)
                decimal_cols = ['90s_col', 'Gls_1', 'Ast_1']
                for col in decimal_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                # d) Conversion de la date
                df['market_value_last_update'] = pd.to_datetime(
                    df['market_value_last_update'], 
                    format='%d/%m/%Y',
                    errors='coerce'
                )
                
                # Gérer les valeurs NaN/None pour pyodbc
                df = df.where(pd.notna(df), None)
                
                # e) Assurer l'ordre des colonnes du DataFrame (doit correspondre au DDL SQL)
                df_final = df[['Season', 'Rk', 'Player', 'Nation', 'Pos', 'Squad', 
                               'Age', 'Born', 'MP', 'Starts', 'Min', '90s_col', 
                               'Gls', 'Ast', 'CrdY', 'CrdR', 
                               'Gls_1', 'Ast_1', 
                               'market_value_euro_k', 'market_value_last_update']].copy()

                # --- CHARGEMENT (L) ---
                
                # Préparer la requête d'insertion (utiliser le nom SQL [90s] dans la requête)
                sql_columns = ['Season', 'Rk', 'Player', 'Nation', 'Pos', 'Squad', 
                               'Age', 'Born', 'MP', 'Starts', 'Min', '[90s]', # Nom SQL avec crochets
                               'Gls', 'Ast', 'CrdY', 'CrdR', 
                               'Gls_1', 'Ast_1', 
                               'market_value_euro_k', 'market_value_last_update']
                               
                columns_str = ', '.join(sql_columns)
                placeholders = ', '.join(['?' for _ in sql_columns])
                insert_query = f"INSERT INTO {STAGING_TABLE} ({columns_str}) VALUES ({placeholders})"
                
                # Convertir le DataFrame en une liste de tuples
                data_to_insert = [tuple(row) for row in df_final.values]

                # Exécuter l'insertion
                cursor.executemany(insert_query, data_to_insert)
                conn.commit()
                
                rows_count = len(df_final)
                total_rows_loaded += rows_count
                print(f"Chargement réussi : {rows_count} lignes insérées dans {STAGING_TABLE}.")

            except Exception as e:
                conn.rollback()
                print(f"Échec critique du traitement du fichier {filename}. Annulation. Erreur : {e}")

    # Fermer la connexion
    conn.close()
    print(f"\n✅ PROCESSUS ETL TERMINÉ. Total des lignes chargées : {total_rows_loaded}.")

# Lancer le script
extract_transform_load_player_stats()