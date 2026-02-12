import pandas as pd
import os

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

input_path = os.path.join(
    THIS_PATH,
    "results",
    "companies",
    "companies_Gebäudegrunddatensatz_vereinigt.csv"
)

output_path = os.path.join(
    THIS_PATH,
    "results",
    "companies",
    "companies_area_and_units_per_cluster.csv"
)

# --------------------------------------------------
# 1. CSV mit ausgewählten Spalten einlesen
# --------------------------------------------------
cols = [
    "Nr.",
    "Name",
    "place_id",
    "mapular_le",
    "Gebaeudegr",
    "Geschossfl",
    "Cluster"
]

df = pd.read_csv(input_path, sep=",", usecols=cols)

# --------------------------------------------------
# 2. Anzahl Einträge pro place_id berechnen
# --------------------------------------------------
entries_per_place = df.groupby("place_id")["place_id"].transform("count")

# --------------------------------------------------
# 3. Nutzfläche berechnen (Geschossfläche / Anzahl Einträge)
# --------------------------------------------------
df["Geschossfl"] = (df["Geschossfl"] / entries_per_place).round()

# --------------------------------------------------
# 4. Nutzfläche UND Nutzeinheiten pro Cluster aggregieren
# --------------------------------------------------
result_df = (
    df.groupby("Cluster", as_index=False)
      .agg(
          **{
              "Nutzfläche (m²)": ("Geschossfl", "sum"),
              "Nutzeinheiten": ("Nr.", "count")
          }
      )
)

# Optional: runden
# result_df["Nutzfläche (m²)"] = result_df["Nutzfläche (m²)"].round(2)

# --------------------------------------------------
# 5. CSV speichern
# --------------------------------------------------
result_df.to_csv(output_path, index=False)

print("Fertig! Datei gespeichert unter:", output_path)