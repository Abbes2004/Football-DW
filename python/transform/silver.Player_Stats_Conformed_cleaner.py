import pyodbc
import pandas as pd
import numpy as np

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

def simplify_player_position(df):
    """
    Simplifie la colonne Position en ne conservant que la première position 
    si elle contient des virgules (ex: 'FW,MF' -> 'FW').
    """
    print("Début de la simplification des positions des joueurs...")
    
    # Remplacer les NaN par une chaîne vide pour éviter les erreurs de .str.split
    df['Position'] = df['Position'].fillna('').astype(str).str.strip()
    
    # Appliquer la logique de split : prend le premier élément avant la virgule
    df['Position_Simplified'] = df['Position'].apply(
        lambda x: x.split(',')[0].strip() if ',' in x else x
    )
    
    # Afficher un résumé des changements (optionnel)
    changes_count = (df['Position'] != df['Position_Simplified']).sum()
    print(f"-> {changes_count} positions ont été simplifiées (ex: FW,MF -> FW).")
    
    # Mettre à jour la colonne originale
    df['Position'] = df['Position_Simplified']
    df.drop(columns=['Position_Simplified'], inplace=True)
    
    return df

def run_position_correction():
    """Charge, corrige la position, et recharge la table silver.Player_Stats_Conformed."""
    conn = None
    try:
        conn = pyodbc.connect(CONN_STRING, autocommit=False)
        print(f"Connexion à la base de données établie pour {PLAYER_STATS_TABLE}.")
        
        # 1. Extraction des données
        sql_select = f"SELECT * FROM {PLAYER_STATS_TABLE};"
        df = pd.read_sql(sql_select, conn)
        
        # 2. Transformation
        df_transformed = simplify_player_position(df.copy())
        
        # 3. Chargement (Mise à jour de la table Silver)
        cursor = conn.cursor()
        
        # Vider la table pour le rechargement (facilite la gestion des types de données)
        print(f"Suppression des données existantes dans {PLAYER_STATS_TABLE}...")
        cursor.execute(f"TRUNCATE TABLE {PLAYER_STATS_TABLE};")
        
        # Reconstruire la requête d'insertion
        columns = df_transformed.columns.tolist()
        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO {PLAYER_STATS_TABLE} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Préparation des données pour pyodbc (remplace NaN/NaT par None)
        data_to_insert = [
            tuple(row) for row in df_transformed.replace({np.nan: None, pd.NaT: None}).values
        ]
        
        print(f"Début du rechargement de {len(data_to_insert)} lignes...")
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"🎉 Correction terminée avec succès. {len(data_to_insert)} lignes rechargées dans {PLAYER_STATS_TABLE}.")
        
    except pyodbc.Error as ex:
        print(f"❌ Erreur lors de la correction : {ex.args[0]}")
    except Exception as e:
        print(f"❌ Erreur Critique inattendue : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    run_position_correction()