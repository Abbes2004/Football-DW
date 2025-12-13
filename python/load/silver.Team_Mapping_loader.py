import pyodbc
from typing import Dict, List, Tuple

# --- 1. CONFIGURATION DU PROJET ---

# Informations de Connexion SQL Server
SQL_SERVER_NAME = '(localdb)\\MSSQLLocalDB'
DATABASE_NAME = 'DW_Football_Staging'
MAPPING_TABLE = 'silver.Team_Mapping'

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Liste des noms d'équipe standardisés (pour référence)
STANDARD_TEAMS_FINAL = {
    "Arsenal", "Aston Villa FC", "AFC Bournemouth", "Brentford FC", "Brighton & Hove Albion FC", "Burnley FC", 
    "Cardiff City FC", "Chelsea FC", "Crystal Palace FC", "Everton FC", "Fulham FC", "Huddersfield Town AFC", 
    "Hull City AFC", "Ipswich Town FC", "Leeds United FC", "Leicester City FC", "Liverpool FC", 
    "Luton Town FC", "Manchester City FC", "Manchester United FC", "Middlesbrough FC", "Newcastle United FC", 
    "Norwich City FC", "Nottingham Forest FC", "Queens Park Rangers FC (QPR)", "Sheffield United FC", 
    "Southampton FC", "Stoke City FC", "Sunderland AFC", "Swansea City AFC", "Tottenham Hotspur FC", 
    "Watford FC", "West Bromwich Albion FC (West Brom)", "West Ham United FC", "Wolverhampton Wanderers FC (Wolves)"
}


# --- 2. DICTIONNAIRE DE MAPPING EXPLICITE (Team_Source_Name -> Team_Standard_Name) ---
# Ce dictionnaire contient toutes les correspondances Source -> Standard basées sur votre analyse
EXPLICIT_TEAM_MAPPING: Dict[str, str] = {
    # Match Details & Long Names
    "Arsenal": "Arsenal",
    "Aston Villa": "Aston Villa FC",
    "AFC Bournemouth": "AFC Bournemouth",
    "Brentford": "Brentford FC",
    "Brighton & Hove Albion": "Brighton & Hove Albion FC",
    "Burnley": "Burnley FC",
    "Cardiff City": "Cardiff City FC",
    "Chelsea": "Chelsea FC",
    "Crystal Palace": "Crystal Palace FC",
    "Everton": "Everton FC",
    "Fulham": "Fulham FC",
    "Huddersfield Town": "Huddersfield Town AFC",
    "Hull City": "Hull City AFC",
    "Ipswich Town": "Ipswich Town FC",
    "Leeds United": "Leeds United FC",
    "Leicester City": "Leicester City FC",
    "Liverpool": "Liverpool FC",
    "Luton Town": "Luton Town FC",
    "Manchester City": "Manchester City FC",
    "Manchester United": "Manchester United FC",
    "Middlesbrough": "Middlesbrough FC",
    "Newcastle United": "Newcastle United FC",
    "Norwich City": "Norwich City FC",
    "Nottingham Forest": "Nottingham Forest FC",
    "Queens Park Rangers": "Queens Park Rangers FC (QPR)",
    "Sheffield United": "Sheffield United FC",
    "Southampton": "Southampton FC",
    "Stoke City": "Stoke City FC",
    "Sunderland": "Sunderland AFC",
    "Swansea City": "Swansea City AFC",
    "Tottenham Hotspur": "Tottenham Hotspur FC",
    "Watford": "Watford FC",
    "West Bromwich Albion": "West Bromwich Albion FC (West Brom)",
    "West Ham United": "West Ham United FC",
    "Wolverhampton Wanderers": "Wolverhampton Wanderers FC (Wolves)",
    
    # History & Matches & Short Names
    "Man City": "Manchester City FC",
    "Stoke": "Stoke City FC",
    "Hull": "Hull City AFC",
    "Bournemouth": "AFC Bournemouth",
    "Wolves": "Wolverhampton Wanderers FC (Wolves)",
    "Huddersfield": "Huddersfield Town AFC",
    "QPR": "Queens Park Rangers FC (QPR)",
    "Newcastle": "Newcastle United FC",
    "Norwich": "Norwich City FC",
    "West Ham": "West Ham United FC",
    "Nott'm Forest": "Nottingham Forest FC",
    "Brighton": "Brighton & Hove Albion FC",
    "Leicester": "Leicester City FC",
    "Middlesbrough": "Middlesbrough FC",
    "Man United": "Manchester United FC",
    "West Brom": "West Bromwich Albion FC (West Brom)",
    "Luton": "Luton Town FC",
    "Tottenham": "Tottenham Hotspur FC",
    "Swansea": "Swansea City AFC",
    "Ipswich": "Ipswich Town FC",
    "Cardiff": "Cardiff City FC",
    
    # League Table Overall (Utd/Forest variants)
    "Manchester Utd": "Manchester United FC",
    "Newcastle Utd": "Newcastle United FC",
    "Nott'ham Forest": "Nottingham Forest FC",
    "Sheffield Utd": "Sheffield United FC",
}


# --- 3. FONCTION D'INSERTION SÉCURISÉE ---

def populate_team_mapping(conn_string: str, mapping_data: Dict[str, str], mapping_table: str):
    """
    Exécute TRUNCATE TABLE et insère le dictionnaire de correspondances.
    """
    if not mapping_data:
        print("Erreur : Le dictionnaire de mapping est vide. Aucune insertion effectuée.")
        return

    # Convertir le dictionnaire en liste de tuples (Source, Standard) pour executemany
    insertion_data: List[Tuple[str, str]] = list(mapping_data.items())
    
    try:
        with pyodbc.connect(conn_string) as conn:
            # Démarrer une transaction
            conn.autocommit = False 
            with conn.cursor() as cursor:
                print(f"Connexion à SQL Server établie sur [{DATABASE_NAME}].")

                # 1. EXÉCUTION DE LA COMMANDE TRUNCATE (Demandée par l'utilisateur)
                truncate_command = f"TRUNCATE TABLE {mapping_table};"
                print(f"Exécution : {truncate_command}")
                cursor.execute(truncate_command) 
                
                # 2. Préparation de la requête d'insertion
                insert_query = f"INSERT INTO {mapping_table} (Team_Source_Name, Team_Standard_Name) VALUES (?, ?)"
                
                # 3. Exécution de l'insertion 
                cursor.executemany(insert_query, insertion_data)
                
                # 4. Validation de la transaction
                conn.commit()
                
                print(f"\n✅ CHARGEMENT TERMINÉ. {len(insertion_data)} paires (Source -> Standard) insérées dans {mapping_table}.")

    except Exception as e:
        print(f"Échec critique de l'insertion dans la table {mapping_table}. Erreur : {e}")
        # Annulation en cas d'erreur
        try:
            conn.rollback()
            print("Transaction annulée.")
        except:
            pass

# --- 4. EXÉCUTION DU SCRIPT ---
if __name__ == "__main__":
    
    print("--- DÉBUT DE LA CRÉATION EXPLICITE DE silver.Team_Mapping ---")
    populate_team_mapping(CONN_STRING, EXPLICIT_TEAM_MAPPING, MAPPING_TABLE)