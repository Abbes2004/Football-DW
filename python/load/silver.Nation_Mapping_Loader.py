import pyodbc
from typing import Dict, List, Tuple

# --- 1. CONFIGURATION DU PROJET ---

# Informations de Connexion SQL Server
SQL_SERVER_NAME = '(localdb)\\MSSQLLOCALDB'
DATABASE_NAME = 'DW_Football_Staging'
MAPPING_TABLE = 'silver.Nation_Mapping'

# Chaîne de connexion SQL Server
CONN_STRING = (
    f'Driver={{ODBC Driver 17 for SQL Server}};'
    f'Server={SQL_SERVER_NAME};'
    f'Database={DATABASE_NAME};'
    f'Trusted_Connection=yes;'
)

# Dictionnaire de mapping fourni par l'utilisateur (Source Key -> Standard Name)
country_dict = {
    "uzUZB": "Ouzbékistan", "slSLE": "Sierra Leone", "gaGAB": "Gabon", "gtGUA": "Guatemala", "iqIRQ": "Irak",
    "engENG": "Angleterre", "ngNGA": "Nigéria", "chSUI": "Suisse", "gnGUI": "Guinée", "usUSA": "États-Unis",
    "deGER": "Allemagne", "egEGY": "Égypte", "meMNE": "Monténégro", "dkDEN": "Danemark", "atAUT": "Autriche",
    "nirNIR": "Irlande du Nord", "roROU": "Roumanie", "siSVN": "Slovénie", "czCZE": "République tchèque",
    "aoANG": "Angola", "beBEL": "Belgique", "cuCUB": "Cuba", "isISL": "Islande", "sctSCO": "Écosse",
    "zwZIM": "Zimbabwe", "brBRA": "Brésil", "tzTAN": "Tanzanie", "skSVK": "Slovaquie", "arARG": "Argentine",
    "amARM": "Arménie", "gdGRN": "Grenade", "clCHI": "Chili", "plPOL": "Pologne", "alALB": "Albanie",
    "frFRA": "France", "veVEN": "Venezuela", "snSEN": "Sénégal", "cnCHN": "Chine", "auAUS": "Australie",
    "keKEN": "Kenya", "htHAI": "Haïti", "hrCRO": "Croatie", "pePER": "Pérou", "tnTUN": "Tunisie",
    "bdBAN": "Bangladesh", "bfBFA": "Burkina Faso", "gqEQG": "Guinée équatoriale", "baBIH": "Bosnie-Herzégovine",
    "xkKVX": "Kosovo", "ghGHA": "Ghana", "msMSR": "Montserrat", "mkMKD": "Macédoine du Nord", "zaRSA": "Afrique du Sud",
    "gwGNB": "Guinée-Bissau", "agATG": "Antigua-et-Barbuda", "uyURU": "Uruguay", "cdCOD": "République démocratique du Congo",
    "phPHI": "Philippines", "nzNZL": "Nouvelle-Zélande", "seSWE": "Suède", "irIRN": "Iran", "ciCIV": "Côte d’Ivoire",
    "eeEST": "Estonie", "krKOR": "Corée du Sud", "cyCYP": "Chypre", "knSKN": "Saint-Christophe-et-Niévès",
    "grGRE": "Grèce", "maMAR": "Maroc", "hnHON": "Honduras", "crCRC": "Costa Rica", "ltLTU": "Lituanie",
    "fiFIN": "Finlande", "jpJPN": "Japon", "dzALG": "Algérie", "cmCMR": "Cameroun", "coCOL": "Colombie",
    "nlNED": "Pays-Bas", "gmGAM": "Gambie", "bjBEN": "Bénin", "noNOR": "Norvège", "ilISR": "Israël",
    "tgTOG": "Togo", "caCAN": "Canada", "ptPOR": "Portugal", "rsSRB": "Serbie", "mlMLI": "Mali",
    "zmZAM": "Zambie", "mxMEX": "Mexique", "uaUKR": "Ukraine", "ieIRL": "Irlande", "idIDN": "Indonésie",
    "ecECU": "Équateur", "biBDI": "Burundi", "pyPAR": "Paraguay", "trTUR": "Turquie", "cwCUW": "Curaçao",
    "bmBER": "Bermudes", "mrMTN": "Mauritanie", "esESP": "Espagne", "wlsWAL": "Pays de Galles", "itITA": "Italie",
    "doDOM": "République dominicaine", "jmJAM": "Jamaïque", "huHUN": "Hongrie"
}


def prepare_nation_data(raw_dict: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Prépare les données pour l'insertion SQL au format (Nation_Source_Key, Nation_Standard_Name).
    """
    # Conversion du dictionnaire en liste de tuples (clé, valeur)
    return list(raw_dict.items())


def populate_nation_mapping(conn_string: str, mapping_data: List[Tuple[str, str]], mapping_table: str):
    """
    Vide la table de mapping et insère les données préparées.
    """
    if not mapping_data:
        print("Erreur : Les données de mapping sont vides. Aucune insertion effectuée.")
        return

    try:
        with pyodbc.connect(conn_string) as conn:
            conn.autocommit = False 
            with conn.cursor() as cursor:
                print(f"Connexion à SQL Server établie sur [{DATABASE_NAME}].")

                # 1. EXÉCUTION DE LA COMMANDE TRUNCATE
                truncate_command = f"TRUNCATE TABLE {mapping_table};"
                print(f"Exécution : {truncate_command}")
                cursor.execute(truncate_command) 
                
                # 2. Préparation de la requête d'insertion (deux colonnes seulement)
                insert_query = f"INSERT INTO {mapping_table} (Nation_Source_Key, Nation_Standard_Name) VALUES (?, ?)"
                
                # 3. Exécution de l'insertion 
                cursor.executemany(insert_query, mapping_data)
                
                # 4. Validation de la transaction
                conn.commit()
                
                print(f"\n✅ CHARGEMENT TERMINÉ. {len(mapping_data)} nations insérées dans {mapping_table}.")

    except Exception as e:
        print(f"Échec critique de l'insertion dans la table {mapping_table}. Erreur : {e}")
        try:
            conn.rollback()
            print("Transaction annulée.")
        except:
            pass

# --- 4. EXÉCUTION DU SCRIPT ---
if __name__ == "__main__":
    
    # Étape 1 : Préparation des données
    data_to_insert = prepare_nation_data(country_dict)
    
    print("--- DÉBUT DE LA CRÉATION EXPLICITE DE silver.Nation_Mapping ---")
    
    # Étape 2 : Chargement dans SQL Server
    populate_nation_mapping(CONN_STRING, data_to_insert, MAPPING_TABLE)