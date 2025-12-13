import pyodbc
from typing import Dict, List, Tuple

# --- 1. CONFIGURATION DU PROJET ---

# Informations de Connexion SQL Server
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'
MAPPING_TABLE = 'silver.Notes_Mapping'

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Dictionnaire de mapping fourni par l'utilisateur (Source Key -> Standard Name)
team_notes_dict = {
    "? Champions League": "Champions League",
    "? Champions League via Europa League win": "Champions League (via Europa League)",
    "? Champions League via league finish": "Champions League (via league finish)",
    "? Conference League via league finish": "Conference League (via league finish)",
    "? Europa Conference League via league finish": "Europa Conference League (via league finish)",
    "? Europa Conference League via league finish1": "Europa Conference League (via league finish)",
    "? Europa League via cup win": "Europa League (via cup)",
    "? Europa League via cup win1": "Europa League (via cup)",
    "? Europa League via cup win2": "Europa League (via cup)",
    "? Europa League via league finish": "Europa League (via league finish)",
    "? Europa League via league finish1": "Europa League (via league finish)",
    "? Europa League via league finish2": "Europa League (via league finish)",
    "? Europa League via league finish3": "Europa League (via league finish)",
    "? Europa League3": "Europa League (via league finish)",
    "4-point deduction2": "4-point deduction",
    "8-point deduction1": "8-point deduction",
    "Relegated": "Relegated"
}

# Ajout explicite de la chaîne vide pour les équipes sans événement
# La clé source est '' et le nom standard sera 'None' ou 'No Event'
team_notes_dict[''] = 'No Event' 


def prepare_notes_data(raw_dict: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Prépare les données pour l'insertion SQL au format (Notes_Source_Key, Notes_Standard_Name).
    """
    # Conversion du dictionnaire en liste de tuples (clé, valeur)
    return list(raw_dict.items())


def populate_notes_mapping(conn_string: str, mapping_data: List[Tuple[str, str]], mapping_table: str):
    """
    Vide la table de mapping et insère les données préparées.
    """
    if not mapping_data:
        print("Erreur : Les données de mapping sont vides. Aucune insertion effectuée.")
        return

    try:
        conn = pyodbc.connect(conn_string)
        conn.autocommit = False 
        cursor = conn.cursor()
        
        print(f"Connexion à SQL Server établie sur [{DATABASE_NAME}].")

        # 1. TRUNCATE (Vider la table)
        truncate_command = f"TRUNCATE TABLE {mapping_table};"
        print(f"Exécution : {truncate_command}")
        cursor.execute(truncate_command) 
        
        # 2. Préparation de la requête d'insertion (deux colonnes)
        insert_query = f"INSERT INTO {mapping_table} (Notes_Source_Key, Notes_Standard_Name) VALUES (?, ?)"
        
        # 3. Exécution de l'insertion 
        cursor.executemany(insert_query, mapping_data)
        
        # 4. Validation de la transaction
        conn.commit()
        
        print(f"\n✅ CHARGEMENT TERMINÉ. {len(mapping_data)} notes/qualifications insérées dans {mapping_table}.")

    except Exception as e:
        print(f"Échec critique de l'insertion dans la table {mapping_table}. Erreur : {e}")
        try:
            conn.rollback()
            print("Transaction annulée.")
        except:
            pass
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# --- 3. EXÉCUTION DU SCRIPT ---
if __name__ == "__main__":
    
    # Étape 1 : Préparation des données
    data_to_insert = prepare_notes_data(team_notes_dict)
    
    print("--- DÉBUT DE LA CRÉATION EXPLICITE DE silver.Notes_Mapping ---")
    
    # Étape 2 : Chargement dans SQL Server
    populate_notes_mapping(CONN_STRING, data_to_insert, MAPPING_TABLE)