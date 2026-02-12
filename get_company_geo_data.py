import os
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.extra.rate_limiter import RateLimiter
from urllib.parse import unquote
import html

this_path = os.path.dirname(os.path.abspath(__file__))
companies_path = os.path.join(this_path, "results", "adlershof_companies.csv")
companies_geodata = os.path.join(this_path, "results", "adlershof_companies_geodata.csv")

# --- 1) CSV einlesen mit Fallback-Encoding ---
def read_csv_with_fallback(path):
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        # Manche Windows-Exporte sind cp1252 (latin-1) kodiert
        return pd.read_csv(path, encoding="cp1252")

companies = read_csv_with_fallback(companies_path)

# --- 2) Setup Geocoder (Rate Limiting bleibt konservativ) ---
geolocator = Nominatim(user_agent="adlershof-geocoder")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=5, max_retries=3, error_wait_seconds=10)

# --- 3) Helfer: Mojibake reparieren ---
def fix_mojibake(s):
    """
    Versucht typische Mojibake-Fälle zu reparieren:
      - Wenn s None, zurück None
      - Wenn s Zeichen wie 'Ã' enthält, versuchen wir `.encode('cp1252').decode('utf-8')`
      - Sonst s unverändert zurückgeben
    Das fängt die üblichen Fälle wie 'StraÃŸe' -> 'Straße' ab.
    """
    if s is None:
        return None
    # already clean?
    if not isinstance(s, str):
        s = str(s)
    # quick heuristic: oft taucht 'Ã' bei Mojibake auf, aber auch 'Ã¤', 'Ã¶', 'Ã¼', 'ÃŸ'
    if "Ã" in s or "Â" in s:
        try:
            # interpretiere die vorhandenen Zeichen als cp1252/latin1-bytes, dann dekodiere als utf-8
            repaired = s.encode("cp1252").decode("utf-8")
            return repaired
        except Exception:
            # fallback: gib original zurück, wir wollen nicht crashen
            return s
    return s

# --- 4) Adresse extrahieren und korrekt unquote mit utf-8 ---
def extract_address(url):
    if pd.isna(url):
        return None
    # Google Maps Links haben oft '?q=' oder '/place/' etc. Wir behandeln '?q=' wie vorher.
    # Unquote explizit mit encoding utf-8
    try:
        if "?q=" in url:
            raw = url.split("?q=")[-1]
            decoded = unquote(raw, encoding="utf-8", errors="replace")
        else:
            # fallback: unquote der ganzen URL falls keine ?q= vorhanden
            decoded = unquote(url, encoding="utf-8", errors="replace")
        # HTML-Entities (falls vorhanden) entfernen
        decoded = html.unescape(decoded)
        # Mojibake-Reparatur
        decoded = fix_mojibake(decoded)
        # Strip whitespace
        return decoded.strip()
    except Exception:
        return None

# --- 5) Geocoding-Funktion mit Fehlerbehandlung ---
def get_coordinates(address):
    if not address:
        return pd.Series([None, None])
    try:
        location = geocode(address)
        if location:
            return pd.Series([location.latitude, location.longitude])
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Geocoding timeout/service error for '{address}': {e}")
    except Exception as e:
        print(f"Unexpected geocoding error for '{address}': {e}")
    return pd.Series([None, None])

# --- 6) Resume / Checkpoint Logik vorbereiten ---
# Falls bereits existierende geodata vorhanden ist, lade und verwende vorhandene Ergebnisse
if os.path.exists(companies_geodata):
    existing = read_csv_with_fallback(companies_geodata)
    # Wenn Spalten passen, kopieren wir Latitude/Longitude zurück
    if "Latitude" in existing.columns and "Longitude" in existing.columns:
        # Zuordnung per Index: vorausgesetzt Reihenfolge unverändert - alternativ: ein Join auf eindeutige ID nutzen
        companies["Latitude"] = existing.get("Latitude")
        companies["Longitude"] = existing.get("Longitude")

# Erstelle Adresse-Spalte falls noch nicht vorhanden
if "Adresse" not in companies.columns:
    companies["Adresse"] = companies["Google Maps Link"].apply(extract_address)
else:
    # Falls bereits vorhanden, repariere sie trotzdem (Mojibake könnte dort sein)
    companies["Adresse"] = companies["Adresse"].apply(lambda x: fix_mojibake(x) if pd.notna(x) else x)

# Erstelle Platzhalter für Koordinaten
if "Latitude" not in companies.columns:
    companies["Latitude"] = None
if "Longitude" not in companies.columns:
    companies["Longitude"] = None

# --- 7) Verarbeitung mit Checkpointing und finaler Speicherung ---
checkpoint_interval = 10
processed_since_save = 0

try:
    for idx, row in companies.iterrows():
        # Skip, wenn bereits Koordinaten vorhanden
        if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
            continue

        addr = row["Adresse"]
        lat, lon = get_coordinates(addr)
        companies.at[idx, "Latitude"] = lat
        companies.at[idx, "Longitude"] = lon
        processed_since_save += 1

        if processed_since_save >= checkpoint_interval:
            print(f"Checkpoint: speichere Ergebnisse (Index {idx})...")
            companies.to_csv(companies_geodata, index=False)
            processed_since_save = 0

except KeyboardInterrupt:
    print("Abbruch durch User. Speichere Zwischenergebnisse...")
    companies.to_csv(companies_geodata, index=False)
    raise

except Exception as e:
    print(f"Unerwarteter Fehler: {e}. Speichere Zwischenergebnisse...")
    companies.to_csv(companies_geodata, index=False)
    raise

finally:
    print("Fertig — schreibe finale Datei.")
    companies.to_csv(companies_geodata, index=False)

print("Fertig.")
