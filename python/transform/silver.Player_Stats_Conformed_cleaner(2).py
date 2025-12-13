import pyodbc
import pandas as pd
from io import StringIO

# --- CONFIGURATION ---
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'
PLAYER_STATS_TABLE = 'silver.Player_Stats_Conformed' 

CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Dictionnaire de correction : Tronqué -> Nom Complet
NATION_CORRECTION_MAP = {
    'République démocrat': 'République démocratique du Congo',
    'République dominica': 'République dominicaine',
    'Saint-Christophe-et': 'Saint-Christophe-et-Niévès'
}

def correct_nation_names_in_silver():
    """
    Met à jour la colonne Nation_Conformed dans silver.Player_Stats_Conformed 
    en utilisant un mapping de correction.
    """
    conn = None
    try:
        conn = pyodbc.connect(CONN_STRING, autocommit=False)
        cursor = conn.cursor()
        print("Connexion à la base de données établie.")
        
        total_rows_affected = 0
        
        # Parcourir le dictionnaire de correction et appliquer les UPDATES
        for truncated_name, full_name in NATION_CORRECTION_MAP.items():
            
            sql_update = f"""
            UPDATE {PLAYER_STATS_TABLE}
            SET Nation_Conformed = ?
            WHERE Nation_Conformed = ?;
            """
            
            # Exécution de la mise à jour
            cursor.execute(sql_update, full_name, truncated_name)
            rows_affected = cursor.rowcount
            total_rows_affected += rows_affected
            
            print(f"-> Correction '{truncated_name}' -> '{full_name}' : {rows_affected} lignes affectées.")
        
        # Valider les changements
        conn.commit()
        
        print(f"\n🎉 Correction de nationalité terminée. Total de {total_rows_affected} lignes mises à jour dans {PLAYER_STATS_TABLE}.")
        
    except pyodbc.Error as ex:
        print(f"❌ Erreur SQL lors de la correction : {ex}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Erreur Critique inattendue : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    correct_nation_names_in_silver()