import pandas as pd
import glob
import os

# --- Configuration du projet ---
source_dir = r"D:\Abbes\Football-DW-Project\data\processed\premierLeagueMatchOdds"
output_dir = r"D:\Abbes\Football-DW-Project\data\processed\premierLeagueMatchOdds_cleaned"
# Créez le dossier de sortie s'il n'existe pas
os.makedirs(output_dir, exist_ok=True)

# Liste des colonnes qui DOIVENT être converties en nombres (FLOAT)
numeric_cols = [
    'FTHG', 'FTAG', 'HTHG', 'HTAG', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF',
    'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A'
]
# Liste de tous les fichiers CSV dans le dossier source
all_files = glob.glob(os.path.join(source_dir, "*.csv"))

print(f"Début du nettoyage de {len(all_files)} fichiers CSV...")

for filename in all_files:
    print(f"Traitement de : {os.path.basename(filename)}")
    
    try:
        # 1. Chargement du fichier
        df = pd.read_csv(filename)
        
        # 2. Nettoyage et Conversion
        for col in numeric_cols:
            # Tente de convertir la colonne en FLOAT. 
            # Si une valeur n'est pas un nombre (ex: '', 'N/A', '-'), elle est remplacée par NaN (NULL)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 3. Conversion de la colonne Date dans un format SQL-friendly
        # Cela corrige aussi les problèmes de formatage
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 4. Sauvegarde du fichier nettoyé
        output_filepath = os.path.join(output_dir, os.path.basename(filename))
        df.to_csv(output_filepath, index=False)
        
        print(f"-> Sauvegardé dans : {output_filepath}")

    except Exception as e:
        print(f"Erreur lors du traitement de {os.path.basename(filename)}: {e}")

print("\nNettoyage terminé. Les fichiers nettoyés sont prêts pour SSIS.")