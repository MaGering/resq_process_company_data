import requests
from bs4 import BeautifulSoup
import csv
import os
import time

BASE_URL = "https://www.adlershof.de"
PAGE_URL_TEMPLATE = BASE_URL + "/firmensuche-institute/adressverzeichnis/firmen?tx_sitepackage_company%5BcompanyPaginator%5D%5BcurrentPage%5D={}"

CSV_FILENAME = os.path.join("results", "adlershof_companies.csv")
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def get_existing_names():
    """Liest bestehende Namen aus der CSV-Datei."""
    if not os.path.exists(CSV_FILENAME):
        return set()

    with open(CSV_FILENAME, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return set(row["Name"] for row in reader)


def get_company_links_from_page(page_num):
    """Extrahiert alle Firmen auf einer bestimmten Seite."""
    print(f"\n🔎 Durchsuche Seite {page_num}...")
    url = PAGE_URL_TEMPLATE.format(page_num)
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    company_divs = soup.select("div.company__item")
    companies = []

    for div in company_divs:
        name_tag = div.select_one("a.headline.company__title")
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)
        href = name_tag.get("href")
        full_url = BASE_URL + href
        companies.append((name, full_url))

    return companies


def get_company_details(company_url):
    """Besucht die Detailseite und extrahiert Branchen und Google-Maps-Link."""
    time.sleep(1)
    response = requests.get(company_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    # Branchen extrahieren
    h2 = soup.find("h2", string=lambda s: s and "Branchen" in s)
    branches = []
    if h2:
        ul = h2.find_next("ul", class_="bullets")
        if ul:
            branches = [li.get_text(strip=True) for li in ul.find_all("li")]

    # Google Maps Link extrahieren
    maps_tag = soup.find("a", class_="google-maps")
    maps_link = maps_tag["href"] if maps_tag else ""
    # Adresse extrahieren
    address = 1

    return branches, maps_link


def append_to_csv(rows):
    """Speichert neue Zeilen in die bestehende oder neue CSV."""
    file_exists = os.path.exists(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Nr.", "Name", "URL", "Branchenzweig", "Google Maps Link"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    existing_names = get_existing_names()
    all_new_rows = []
    next_id = len(existing_names) + 1

    for page in range(1, 9):  # Seiten 1 bis 8
        companies = get_company_links_from_page(page)
        for name, url in companies:
            if name in existing_names:
                print(f"⏭️  {name} bereits vorhanden – übersprungen.")
                continue

            print(f"➡️  Verarbeite {name}")
            branches, maps_link = get_company_details(url)
            branch_string = ", ".join(branches)
            all_new_rows.append({
                "Nr.": next_id,
                "Name": name,
                "URL": url,
                "Branchenzweig": branch_string,
                "Google Maps Link": maps_link
            })
            next_id += 1

    if all_new_rows:
        append_to_csv(all_new_rows)
        print(f"\n✅ {len(all_new_rows)} neue Einträge wurden zur CSV hinzugefügt.")
    else:
        print("\n📄 Keine neuen Unternehmen gefunden – CSV bleibt unverändert.")


if __name__ == "__main__":
    main()
