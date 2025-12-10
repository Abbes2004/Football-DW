import pandas as pd
import pyodbc
import os
import re

# --- 1. CONFIGURATION DU PROJET ---
# Chemin du dossier source pour les fichiers JSON 'Squad Stats'
JSON_DIRECTORY = r'D:\Abbes\Football-DW-Project\data\processed\epl_squad_stats_json'
JSON_PATTERN = '.json'

# Informations de Connexion SQL Server (Sauvegardées)
SQL_SERVER_NAME = '(localdb)\MSSQLLocalDB'
DATABASE_NAME = 'DW_Football_Staging'
STAGING_TABLE = 'bronze.staging_squad_stats' # <--- Table Cible

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Dictionnaire de mappage JSON vers DDL
# Ces noms seront utilisés dans le DataFrame avant l'insertion
COLUMN_MAPPING = {
    '# Pl': 'Players_Count',
    '90s': 'Ninety_Count',
    'G+A': 'G_plus_A',
    'G-PK': 'G_minus_PK',
    'Gls_1': 'Gls_per_90',      # Assumons Gls_1 = Gls/90
    'Ast_1': 'Ast_per_90',      # Assumons Ast_1 = Ast/90
    'G+A_1': 'GA_per_90',       # Assumons G+A_1 = G+A/90
    'G-PK_1': 'G_minus_PK_90',   # Assumons G-PK_1 = G-PK/90
    'G+A-PK': 'GA_minus_PK'
}

# --- 2. Fonction de Génération de Saison (Mise à jour pour format YYYY-YYYY) ---
# --- 2. Fonction de Génération de Saison (CORRIGÉE pour format YY_YY) ---
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
        return f"{start_year}/{end_year_short}" # Retournera par ex. "2014/15" (7 caractères)
    
    return "Unknown/Season" # Se produit si le nom du fichier est incorrect

# --- 3. FONCTION PRINCIPALE ETL ---
def extract_transform_load_squad_stats():
    """
    Parcourt tous les fichiers JSON, les transforme et les charge dans la table SQUAD STATS de staging.
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
                
                # b) Renommage des colonnes
                df = df.rename(columns=COLUMN_MAPPING)
                
                # c) Nettoyage et Conversion des types
                
                # Nettoyage de la colonne 'Min' (retirer les virgules)
                df['Min'] = df['Min'].astype(str).str.replace(',', '').str.strip()
                
                # Colonnes INT
                int_cols = ['Players_Count', 'MP', 'Starts', 'Min', 'Gls', 'Ast', 'G_plus_A', 'G_minus_PK', 'PK', 'PKatt', 'CrdY', 'CrdR']
                for col in int_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

                # Colonnes DECIMAL (4,1 pour Age, 5,2 pour le reste)
                df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
                
                decimal_cols_5_2 = ['Poss', 'Ninety_Count', 'Gls_per_90', 'Ast_per_90', 'GA_per_90', 'G_minus_PK_90', 'GA_minus_PK']
                for col in decimal_cols_5_2:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                # Gérer les valeurs NaN/None pour pyodbc
                df = df.where(pd.notna(df), None)
                
                # d) Assurer l'ordre des colonnes (DOIT correspondre à l'ordre du DDL SQL)
                df_final = df[['Season', 'Squad', 'Players_Count', 'Age', 'Poss', 'MP', 'Starts', 'Min', 'Ninety_Count', 
                               'Gls', 'Ast', 'G_plus_A', 'G_minus_PK', 'PK', 'PKatt', 'CrdY', 'CrdR', 
                               'Gls_per_90', 'Ast_per_90', 'GA_per_90', 'G_minus_PK_90', 'GA_minus_PK']].copy()

                # --- CHARGEMENT (L) ---
                
                # Préparer la requête d'insertion
                columns = ', '.join(df_final.columns)
                placeholders = ', '.join(['?' for _ in df_final.columns])
                insert_query = f"INSERT INTO {STAGING_TABLE} ({columns}) VALUES ({placeholders})"
                
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
extract_transform_load_squad_stats()