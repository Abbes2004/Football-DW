import pyodbc

# --- CONFIGURATION ---
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'
MATCH_ODDS_TABLE = 'silver.Match_Odds_Conformed'

CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Correction Spécifique
TEAM_OLD = 'Leeds'
TEAM_NEW = 'Leeds United FC'


def correct_team_name_in_silver():
    """
    Met à jour toutes les occurrences de 'Leeds' par 'Leeds United FC'
    dans les colonnes HomeTeam_Conformed et AwayTeam_Conformed.
    """
    conn = None
    try:
        conn = pyodbc.connect(CONN_STRING, autocommit=False)
        cursor = conn.cursor()
        print("Connexion à la base de données établie.")

        # Requête d'UPDATE pour HomeTeam
        sql_home_update = f"""
        UPDATE {MATCH_ODDS_TABLE}
        SET HomeTeam_Conformed = ?
        WHERE HomeTeam_Conformed = ?;
        """

        # Requête d'UPDATE pour AwayTeam
        sql_away_update = f"""
        UPDATE {MATCH_ODDS_TABLE}
        SET AwayTeam_Conformed = ?
        WHERE AwayTeam_Conformed = ?;
        """

        # Exécution pour HomeTeam
        cursor.execute(sql_home_update, TEAM_NEW, TEAM_OLD)
        rows_home_affected = cursor.rowcount
        print(f"-> Colonne HomeTeam_Conformed : {rows_home_affected} lignes corrigées.")

        # Exécution pour AwayTeam
        cursor.execute(sql_away_update, TEAM_NEW, TEAM_OLD)
        rows_away_affected = cursor.rowcount
        print(f"-> Colonne AwayTeam_Conformed : {rows_away_affected} lignes corrigées.")

        total_rows_affected = rows_home_affected + rows_away_affected

        # Valider les changements
        conn.commit()

        print(
            f"\n🎉 Correction de l'équipe '{TEAM_OLD}' par '{TEAM_NEW}' terminée. Total de {total_rows_affected} lignes mises à jour.")

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
    correct_team_name_in_silver()