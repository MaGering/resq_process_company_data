import pandas as pd
import os

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

companies_preprocessed_path = os.path.join(
    THIS_PATH,
    "results",
    "companies",
    "adlershof_companies_geodata_preprocessed.csv")

companies_assigned_cluster_path = os.path.join(THIS_PATH, "results", "companies", "adlershof_companies_processed.csv")

# --------------------------------------------------
# 1. CSV einlesen
# --------------------------------------------------
df = pd.read_csv(companies_preprocessed_path, sep=",")

# Sicherheit: alles lowercase für Matching
df["Branchenzweig_norm"] = df["Branchenzweig"].str.lower().fillna("")


# --------------------------------------------------
# 2. Cluster-Definition (Priorität von oben nach unten!)
# --------------------------------------------------
cluster_keywords = {

    "Alten- / Pflegeheim": [
        "pflegeheim", "pflegedienst",
        "betreutes wohnen", "alten"
    ],

    "Rechenzentrum": [
        "rechenzentrum", "server", "telekommunikation",
        "it-netzwerke", "it-hardware", "hosting"
    ],

    "Labor": [
        "labor", "diagnostik",
        "pharmazie", "biotechnologie",
        "medizintechnik", "therapietechnik",
        "photonik", "optik", "laser",
        "mikrosysteme", "materialien",
        "reinraum", "umweltanalytik / schadstoffanalytik",
        "mems / sensoren"
    ],

    "Krankenhaus": [
        "krankenhaus", "klinik", "radiologie",
        "onkologie", "chirurgie", "internisten",
        "kardiologie", "medizinisches zentrum",
        "arztpraxen",
        "neurologie, psychotherapie / psychiatrie",
        "arbeitsmedizin"
    ],

    "Lagerhalle": [
        "lager", "logistik", "transport",
        "kurierdienste", "großhandel",
        "reinigung", "facility", "service",
        "gebäudereinigung"
    ],

    "Bibliothek": [
        "bibliothek", "archiv"
    ],

    "Schule": [
        "schule", "ausbildung",
        "weiterbildung", "fahrschule",
        "universitäre einrichtungen"
    ],

    "Kindergarten": [
        "kinderbetreuung"
    ],

    "Supermarkt": [
        "supermarkt", "lebensmittel",
        "bäckerei", "fleischerei"
    ],

    "Einkaufszentrum": [
        "einkaufszentrum"
    ],

    "Hotel": [
        "hotels / unterkünfte"
    ],

    "Restaurant": [
        "restaurant", "gastronomie",
        "catering", "event-gastronomie"
    ],

    "Kantine": [
        "kantine"
    ],

    "Fitnesscenter": [
        "fitness", "sportstudio", "sport",
        "yoga", "taekwondo"
    ],

    "Sporthalle": [
        "sporthalle", "sportanlage"
    ],

    "Schwimmbad": [
        "schwimmbad"
    ],

    "Theater": [
        "kultur",
    ],

    "Museum": [
        "museum", "ausstellung"
    ],

    "Einzelhandel": [
        "handel / dienstleistungen",
        "einzelhandel", "handel",
        "buchhandel", "zeitschriftenhandel",
        "fotohandel", "apotheke",
        "sanitätshaus", "augenoptiker",
        "hörakustik", "friseur",
        "kosmetik", "wellness",
        "sporthandel", "autohaus",
        "zentrum", "store", "bike",
        "handel"
    ],

    "Produktion": [
        "produktion", "maschinenbau", "anlagenbau",
        "werkzeugbau", "metallbearbeitung",
        "gerätebau", "automatisierungstechnik",
        "glasherstellung", "glasbearbeitung",
        "recycling", "abfallwirtschaft",
        "umwelttechnologie", "industrie 4.0",
        "manufacturing", "produktion", "industrial",
        "bauausführungen", "fertigung", "systems",
        "bauwesen",
        "it / medien", "elektronik / elektrotechnik",
        "lichttechnik", "klimatechnik / kältetechnik",
        "automobil- / verkehrstechnik",
        "klimatechnik / kältetechnik",
        "luftfahrt / raumfahrt"
    ],

    "Büro": [
        "büro", "unternehmensberatung",
        "wissenschaftliche einrichtungen",
        "technologieberatung", "gutachten",
        "banken", "finanzdienstleistungen",
        "versicherungen", "rechtsberatung",
        "anwälte", "steuerberatung",
        "it-dienstleistungen", "software",
        "werbung", "marketing",
        "projektentwicklung", "immobilien",
        "bezirksämter", "verwaltung",
        "print", "mail",
        "ingenieurdienstleistung",
        "medizinische / soziale einrichtungen",
        "agentur für arbeit", "jobcenter",
        "verein", "stiftung", "software",
        "consulting", "analytics", "engineering",
        "architekt", "ingenieure", "planungsbüro",
        "allgemeine dienstleistungen",
        "energiesysteme, energieversorgung",
        "bühnentechnik", "außeruniversitäre institute",
        "mobilität / e-mobilität", "erneuerbare energien"
    ],


    # --------------------------------------------------
    # INFRASTRUKTUR
    # --------------------------------------------------
    "Parkhaus": [
        "parkhaus", "tiefgarage"
    ]
}


# --------------------------------------------------
# 3. Zuordnungsfunktion
# --------------------------------------------------
def assign_cluster(text):
    for cluster, keywords in cluster_keywords.items():
        for kw in keywords:
            if kw in text:
                return cluster
    return "Sonstiges"


# --------------------------------------------------
# 4. Cluster-Spalte erzeugen
# --------------------------------------------------
df["Cluster"] = df["Branchenzweig_norm"].apply(assign_cluster)

# Händisch Einträge modifizieren
df.loc[df["Name"] == "Hochschulsport Adlershof", "Cluster"] = "Sporthalle"
df.loc[df["Name"] == "Alternate Photonics GmbH", "Cluster"] = "Produktion"

# Hilfsspalte optional löschen
df.drop(columns=["Branchenzweig_norm"], inplace=True)


# --------------------------------------------------
# 5. Optional: speichern
# --------------------------------------------------
df.to_csv(companies_assigned_cluster_path, index=False)

print("✅ Cluster-Spalte erfolgreich erstellt.")