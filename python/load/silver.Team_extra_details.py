import pyodbc
import pandas as pd
import io
import re

# --- 1. CONFIGURATION ---
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'

# Tables
BRONZE_TABLE = 'bronze.staging_league_table_overall'
SILVER_DESTINATION_TABLE = 'silver.Team_extra_details'
TEAM_MAPPING_TABLE = 'silver.Team_Mapping'
NOTES_MAPPING_TABLE = 'silver.Notes_Mapping'

CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# --- 2. FONCTION DE TRANSFORMATION PRINCIPALE ---

def transform_top_scorer(scorer_str):
    """
    Gère l'extraction du nom du joueur et des buts, gérant les noms composés et les doubles buteurs.
    Retourne (player_name, goals)
    """
    if pd.isna(scorer_str) or scorer_str.strip() == '':
        return (None, None)

    scorer_str = scorer_str.strip()

    # 1. Splitter au dernier tiret pour isoler les buts
    # Ex: "James Ward-Prowse-9" -> ["James Ward-Prowse", "9"]
    # Ex: "Rasmus Højlund,Bruno Fernandes-10" -> ["Rasmus Højlund,Bruno Fernandes", "10"]
    parts = scorer_str.rsplit('-', 1)
    
    if len(parts) != 2:
        # Si le format est incorrect (pas de tiret), on retourne None
        return (None, None)

    player_part = parts[0].strip()
    goals_part = parts[1].strip()

    # Tente de convertir les buts en INT
    try:
        goals = int(goals_part)
    except ValueError:
        goals = None

    # 2. Gérer les doubles buteurs (s'il y a une virgule dans la partie joueur)
    if ',' in player_part:
        # On prend seulement le premier buteur (Choix B)
        player_name = player_part.split(',')[0].strip()
    else:
        player_name = player_part

    return (player_name, goals)


def run_etl_to_silver_team_extra_details():
    try:
        conn = pyodbc.connect(CONN_STRING)
        
        # 1. Chargement des données sources et des tables de mapping
        df_bronze = pd.read_sql(f"SELECT Season, Squad, Attendance, Top_Team_Scorer, Goalkeeper, Notes FROM {BRONZE_TABLE}", conn)
        df_map_team = pd.read_sql(f"SELECT Team_Source_Name, Team_Standard_Name FROM {TEAM_MAPPING_TABLE}", conn)
        df_map_notes = pd.read_sql(f"SELECT Notes_Source_Key, Notes_Standard_Name FROM {NOTES_MAPPING_TABLE}", conn)
        
        print("✅ Données chargées dans Pandas.")

        # --- 2. Standardisation des Clés (Lookup/Jointure) ---

        # Jointure 1: Squad -> Team_Conformed
        df_silver = pd.merge(
            df_bronze, 
            df_map_team, 
            left_on='Squad', 
            right_on='Team_Source_Name', 
            how='left'
        ).rename(columns={'Team_Standard_Name': 'Squad_Conformed'})
        df_silver['Squad_Conformed'] = df_silver['Squad_Conformed'].fillna(df_silver['Squad']) # Fallback au nom source

        # Jointure 2: Notes -> Qualification_Notes
        df_silver = pd.merge(
            df_silver, 
            df_map_notes, 
            left_on='Notes', 
            right_on='Notes_Source_Key', 
            how='left'
        ).rename(columns={'Notes_Standard_Name': 'Qualification_Notes'})
        # Remplacer les NULLs de jointure par 'No Event' (ou la valeur vide de votre mapping)
        df_silver['Qualification_Notes'] = df_silver['Qualification_Notes'].fillna('No Event')


        # --- 3. Transformations du Top Scorer et de l'Attendance ---

        # Application de la fonction complexe sur Top_Team_Scorer
        df_silver[['TopScorer_PlayerName', 'TopScorer_Goals']] = df_silver['Top_Team_Scorer'].apply(
            lambda x: pd.Series(transform_top_scorer(x))
        )
        
        # Conversion de l'Attendance en INT
        # Utilisation de pd.to_numeric avec errors='coerce' pour transformer les non-numériques en NaN
        df_silver['Season_Attendance'] = pd.to_numeric(df_silver['Attendance'], errors='coerce').astype('Int64') 

        
        # --- 4. Finalisation et Sélection des Colonnes ---

        # Renommage final pour le Gardien
        df_silver.rename(columns={'Goalkeeper': 'Goalkeeper_PlayerName'}, inplace=True)

        # Sélection des colonnes finales pour la destination
        df_final = df_silver[[
            'Season', 'Squad_Conformed', 'Season_Attendance', 
            'TopScorer_PlayerName', 'TopScorer_Goals', 'Goalkeeper_PlayerName', 
            'Qualification_Notes'
        ]]

        print("✅ Transformations Silver terminées.")
        
        # --- 5. Chargement dans SQL Server (Couche Silver) ---

        # Créer le curseur
        cursor = conn.cursor()

        # Vider la table de destination (approche TRUNCATE pour une table vide)
        cursor.execute(f"TRUNCATE TABLE {SILVER_DESTINATION_TABLE};")
        conn.commit()

        # Préparer la commande INSERT
        insert_query = f"INSERT INTO {SILVER_DESTINATION_TABLE} (Season, Squad_Conformed, Season_Attendance, TopScorer_PlayerName, TopScorer_Goals, Goalkeeper_PlayerName, Qualification_Notes) VALUES (?, ?, ?, ?, ?, ?, ?)"
        
        # Préparer les données pour l'insertion
        # Remplacer les NaT (Not a Time) par None pour que pyodbc gère les NULLs SQL
        data_to_insert = [tuple(row) for row in df_final.replace({pd.NA: None, pd.NaT: None}).values]

        # Insertion
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        
        print(f"🎉 Succès ! {len(data_to_insert)} lignes insérées dans {SILVER_DESTINATION_TABLE}.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"❌ Erreur SQL/ODBC : {sqlstate}")
        print("Annulation de la transaction.")
        if 'conn' in locals() and conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Erreur Critique : {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# --- EXÉCUTION ---
if __name__ == "__main__":
    run_etl_to_silver_team_extra_details()