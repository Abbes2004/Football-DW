import pandas as pd
import re

# --- Configuration (À adapter avec vos chemins absolus) ---
# Chemin vers votre fichier CSV d'entrée (dérivé de l'XML corrompu)
INPUT_CSV_FILE = r'D:\Abbes\proj\processed\processed\match_detail.csv'

# Chemin où le nouveau fichier CSV nettoyé doit être enregistré
OUTPUT_CSV_FILE = r'D:\Abbes\Football-DW-Project\data\processed\match_detail_clean.csv'

# Expression régulière pour identifier le format de saison court (ex: 2018/19 ou 2018/2019)
# Ce format est utilisé uniquement pour la Premier League (PL) dans votre source.
SEASON_PATTERN = re.compile(r'^\d{4}/\d{2,4}$')


def clean_and_filter_csv(input_path, output_path):
    # 1. Lire le fichier CSV
    try:
        # Assurez-vous d'utiliser le bon séparateur si ce n'est pas la virgule
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"Erreur : Le fichier d'entrée '{input_path}' est introuvable.")
        return
    except Exception as e:
        print(f"Erreur lors de la lecture du CSV : {e}")
        return

    # 2. Appliquer le filtre sur la colonne 'Season'

    # Créer un masque booléen qui est VRAI si la colonne 'Season' correspond au format PL
    # La méthode .astype(str) gère les valeurs qui pourraient être lues comme non-chaînes (NaN, etc.)
    filter_mask = df['Season'].astype(str).apply(lambda x: bool(SEASON_PATTERN.match(x.strip())))

    # Appliquer le masque pour ne garder que les lignes de la Premier League
    df_filtered = df[filter_mask]

    # 3. Enregistrer le nouveau fichier CSV
    df_filtered.to_csv(output_path, index=False)

    rows_input = len(df)
    rows_kept = len(df_filtered)
    rows_filtered = rows_input - rows_kept

    print(f"--- Résultat du Nettoyage ---")
    print(f"Lignes lues au total : {rows_input}")
    print(f"Lignes conservées (Premier League) : {rows_kept}")
    print(f"Lignes filtrées (Autres Ligues) : {rows_filtered}")
    print(f"Le fichier nettoyé est enregistré sous : {output_path}")


# --- Exécution du Script ---
clean_and_filter_csv(INPUT_CSV_FILE, OUTPUT_CSV_FILE)