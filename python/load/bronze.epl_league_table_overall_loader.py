import pandas as pd
import pyodbc
import os
import re

# --- 1. CONFIGURATION DU PROJET ---
# Chemin du dossier source pour les fichiers JSON 'Overall'
JSON_DIRECTORY = r'D:\Abbes\Football-DW-Project\data\processed\epl_league_table_overall_json'
JSON_PATTERN = '.json'

# Informations de Connexion SQL Server (Sauvegardées)
SQL_SERVER_NAME = '(localdb)\MSSQLLocalDB'
DATABASE_NAME = 'DW_Football_Staging'
STAGING_TABLE = 'bronze.staging_league_table_overall' # <--- Table Cible

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# --- 2. Fonction de Génération de Saison ---
def generate_season_from_filename(filename):
    """
    Extrait l'année de la saison du nom de fichier (ex: '..._14_15.json' -> '2014/15').
    """
    # Regex pour trouver le motif de fin '_YY_YY'
    match = re.search(r'\_(\d{2})\_(\d{2})\.json$', filename)
    if match:
        start_year_short = match.group(1)
        end_year_short = match.group(2)
        
        # Supposons que les années sont 20xx
        start_year = "20" + start_year_short
        return f"{start_year}/{end_year_short}"
    
    return "Unknown/Season" 

# --- 3. FONCTION PRINCIPALE ETL ---
def extract_transform_load_overall_table():
    """
    Parcourt tous les fichiers JSON, les transforme et les charge dans la table OVERALL de staging.
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
            season = generate_season_from_filename(filename)
            print(f"\n--- Traitement du fichier : {filename} (Saison: {season}) ---")

            try:
                # --- EXTRACTION (E) ---
                df = pd.read_json(file_path)
                
                # --- TRANSFORMATION (T) ---
                
                # a) Créer la colonne Season 
                df.insert(0, 'Season', season)
                
                # b) Renommer les colonnes pour correspondre au DDL
                df = df.rename(columns={
                    'Pts/MP': 'Pts_per_MP',
                    'Top Team Scorer': 'Top_Team_Scorer'
                })
                
                # c) Conversion des types et nettoyage
                
                # DDL: Rk, MP, W, D, L, GF, GA, Pts, Attendance sont INT
                int_cols = ['Rk', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'Pts', 'Attendance']
                for col in int_cols:
                    # Nettoyage des chaînes avant conversion numérique (retirer les virgules de l'Attendance)
                    if col == 'Attendance':
                        df[col] = df[col].astype(str).str.replace(',', '').str.strip()
                    
                    # Conversion en INT, gère les erreurs en NaN puis remplace par 0
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

                # DDL: Pts_per_MP est DECIMAL(4,2)
                df['Pts_per_MP'] = pd.to_numeric(df['Pts_per_MP'], errors='coerce')
                
                # Assurez-vous que les colonnes string (GD, Scorer, Goalkeeper, Notes) gèrent bien les valeurs nulles
                
                # Gérer les valeurs NaN/None pour pyodbc (très important avant l'insertion)
                df = df.where(pd.notna(df), None)
                
                # d) Assurer l'ordre des colonnes (DOIT correspondre à l'ordre du DDL)
                df = df[['Season', 'Rk', 'Squad', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts', 'Pts_per_MP', 
                         'Attendance', 'Top_Team_Scorer', 'Goalkeeper', 'Notes']]
                
                # --- CHARGEMENT (L) ---
                
                # Préparer la requête d'insertion
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['?' for _ in df.columns])
                insert_query = f"INSERT INTO {STAGING_TABLE} ({columns}) VALUES ({placeholders})"
                
                # Convertir le DataFrame en une liste de tuples
                data_to_insert = [tuple(row) for row in df.values]
                
                # Exécuter l'insertion
                cursor.executemany(insert_query, data_to_insert)
                conn.commit()
                
                rows_count = len(df)
                total_rows_loaded += rows_count
                print(f"Chargement réussi : {rows_count} lignes insérées dans {STAGING_TABLE}.")

            except Exception as e:
                conn.rollback()
                print(f"Échec critique du traitement du fichier {filename}. Annulation. Erreur : {e}")

    # Fermer la connexion
    conn.close()
    print(f"\n✅ PROCESSUS ETL TERMINÉ. Total des lignes chargées : {total_rows_loaded}.")

# Lancer le script
extract_transform_load_overall_table()