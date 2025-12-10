import pandas as pd
import pyodbc
import os
import re

# --- 1. CONFIGURATION SSIS/SQL SERVER (Sauvegardée) ---
JSON_DIRECTORY = r'D:\Abbes\Football-DW-Project\data\processed\epl_league_table_away_json'
JSON_PATTERN = '.json'

# Informations de Connexion SQL Server
SQL_SERVER_NAME = '(localdb)\MSSQLLocalDB'
DATABASE_NAME = 'DW_Football_Staging'
STAGING_TABLE = 'bronze.staging_league_table_away'

# Pilote standard pour (localdb)
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
    
    # Retourne une valeur par défaut ou lève une erreur si le format n'est pas trouvé
    return "Unknown/Season" 

# --- 3. FONCTION PRINCIPALE ETL ---
def extract_transform_load_league_table():
    """
    Parcourt tous les fichiers JSON, les transforme et les charge dans la table de staging.
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
                # Le JSON contient une liste d'objets, pd.read_json le lit directement en DataFrame
                df = pd.read_json(file_path)
                
                # --- TRANSFORMATION (T) ---
                
                # a) Créer la colonne Season (Manquante dans le JSON)
                df.insert(0, 'Season', season)
                
                # b) Renommer la colonne 'Pts/MP' pour correspondre au DDL 'Pts_per_MP'
                df = df.rename(columns={'Pts/MP': 'Pts_per_MP'})
                
                # c) Conversion des types (Important pour DDL)
                # Tous les champs numériques sont lus comme des chaînes ("1", "2.00"). Il faut les caster.
                
                # DDL: Rk, MP, W, D, L, GF, GA, Pts sont INT
                int_cols = ['Rk', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'Pts']
                for col in int_cols:
                    # Conversion en INT, gère les erreurs en NaN puis remplace par 0 (ou None)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

                # DDL: Pts_per_MP est DECIMAL(4,2)
                df['Pts_per_MP'] = pd.to_numeric(df['Pts_per_MP'], errors='coerce')

                # Gérer les valeurs NaN/None pour pyodbc (pyodbc préfère None plutôt que numpy.nan)
                df = df.where(pd.notna(df), None)
                
                # Assurer l'ordre des colonnes du DataFrame avant l'insertion (doit correspondre au DDL)
                df = df[['Season', 'Rk', 'Squad', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts', 'Pts_per_MP']]
                
                # --- CHARGEMENT (L) ---
                
                # Préparer la requête d'insertion
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['?' for _ in df.columns])
                insert_query = f"INSERT INTO {STAGING_TABLE} ({columns}) VALUES ({placeholders})"
                
                # Convertir le DataFrame en une liste de tuples pour l'insertion
                data_to_insert = [tuple(row) for row in df.values]
                
                # Exécuter l'insertion
                cursor.executemany(insert_query, data_to_insert)
                conn.commit()
                
                rows_count = len(df)
                total_rows_loaded += rows_count
                print(f"Chargement réussi : {rows_count} lignes insérées dans {STAGING_TABLE}.")

            except Exception as e:
                conn.rollback() # Annuler l'insertion en cas d'erreur
                print(f"Échec critique du traitement du fichier {filename}. Annulation. Erreur : {e}")

    # Fermer la connexion
    conn.close()
    print(f"\n✅ PROCESSUS ETL TERMINÉ. Total des lignes chargées : {total_rows_loaded}.")

# Lancer le script
extract_transform_load_league_table()