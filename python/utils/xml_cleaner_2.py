import pandas as pd
import re

# --- Configuration (À MODIFIER avec vos chemins absolus) ---
# Fichier CSV contenant potentiellement d'autres ligues et des erreurs de score.
INPUT_CSV_FILE = r'D:\Abbes\Football-DW-Project\data\raw\match_detail.csv'

# Fichier CSV de sortie, prêt pour SSIS (Bronze Layer).
OUTPUT_CSV_FILE = r'D:\Abbes\Football-DW-Project\data\processed\match_detail_pl_clean.csv'

# Expression régulière pour identifier le format de saison court (PL)
SEASON_PATTERN = re.compile(r'^\d{4}/\d{2,4}$')

# Colonnes contenant des scores qui doivent être des nombres entiers
SCORE_COLUMNS = ['Home_team_score', 'Away_team_score']


def fix_all_csv_errors(input_path, output_path):
    # 1. Lire le fichier CSV
    try:
        # Lire avec l'option 'keep_default_na=False' pour traiter les chaînes vides
        # (souvent lues comme NaN par défaut) comme des chaînes vides
        df = pd.read_csv(input_path, keep_default_na=False)
    except FileNotFoundError:
        print(f"Erreur : Le fichier d'entrée '{input_path}' est introuvable.")
        return
    except Exception as e:
        print(f"Erreur lors de la lecture du CSV : {e}")
        return

    rows_input = len(df)

    # 2. Nettoyage et Filtration par Colonne 'Season' (Gestion des autres ligues)
    print("-> Étape 1 : Filtrage des matchs de la Premier League...")

    # Créer un masque pour ne garder que les saisons au format 'YYYY/YY'
    filter_mask = df['Season'].astype(str).apply(
        lambda x: bool(SEASON_PATTERN.match(x.strip()))
    )
    df_filtered = df[filter_mask].copy()  # Utilisation de .copy() pour éviter les SettingWithCopyWarning

    rows_filtered_by_season = rows_input - len(df_filtered)
    print(f"   -> {rows_filtered_by_season} lignes filtrées (Autres Ligues).")

    # 3. Nettoyage des Colonnes de Score (Gestion de l'erreur 0xC02020C5)
    print("-> Étape 2 : Nettoyage des scores (Home_team_score, Away_team_score)...")

    for col in SCORE_COLUMNS:
        # Remplacer les chaînes vides, les espaces, et autres valeurs non numériques par 0

        # Tentative de conversion en numérique, en forçant les erreurs à NaN
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

        # Remplacer les NaN (qui incluent les valeurs textuelles non convertibles) par 0
        df_filtered[col] = df_filtered[col].fillna(0).astype(int)

        print(f"   -> Colonne '{col}' nettoyée.")

    # 4. Enregistrement du Fichier Nettoyé
    rows_kept = len(df_filtered)
    print(f"-> Étape 3 : Enregistrement du fichier nettoyé...")

    df_filtered.to_csv(output_path, index=False)

    print(f"--- Résultat Final du Nettoyage ---")
    print(f"Lignes d'entrée : {rows_input}")
    print(f"Lignes conservées (PL, Scores OK) : {rows_kept}")
    print(f"Le fichier nettoyé est enregistré sous : {output_path}")


# --- Exécution du Script ---
fix_all_csv_errors(INPUT_CSV_FILE, OUTPUT_CSV_FILE)