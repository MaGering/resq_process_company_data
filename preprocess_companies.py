import pandas as pd
import os
import re
import string


THIS_PATH = os.path.dirname(os.path.abspath(__file__))

input_path = os.path.join(
    THIS_PATH,
    "results",
    "companies",
    "adlershof_companies_geodata.csv"
)

results_path = os.path.join(
    THIS_PATH,
    "results",
    "companies",
    "adlershof_companies_geodata_preprocessed.csv"
)


# --------------------------------------------------
# 1. CSV einlesen
# --------------------------------------------------
df = pd.read_csv(input_path, sep=",")



# --------------------------------------------------
# 2. Adresse preprocessing
# --------------------------------------------------

def clean_and_expand_adresse(row):
    adresse = str(row["Adresse"]).strip()

    # ------------------------------------------
    # Fix typo
    # ------------------------------------------
    adresse = adresse.replace("Chausee", "Chaussee")

    # ------------------------------------------
    # Add missing PLZ if not present
    # ------------------------------------------
    if not re.match(r"^\d{5}\s", adresse):
        adresse = f"12489 Berlin {adresse}"

    # ------------------------------------------
    # Remove company prefix after "Berlin"
    # Example:
    # 12489 Berlin ZPV, Johann-Hittorf-Straße 8
    # Only if what follows comma contains a number
    # ------------------------------------------
    match = re.match(r"^(\d{5}\sBerlin)\s([^,]+),\s(.+)", adresse)
    if match and "Haus" not in match.string and "OG" not in match.string:
        plz_part = match.group(1)
        after_comma = match.group(3)

        if re.search(r"\d", after_comma):
            adresse = f"{plz_part} {after_comma}"

    # ------------------------------------------
    # Remove everything after first comma
    # (this fixes Haus, OG, company prefixes, etc.)
    # ------------------------------------------
    adresse = adresse.split(",")[0].strip()

    # ------------------------------------------
    # Remove everything after "("
    # ------------------------------------------
    adresse = adresse.split("(")[0].strip()

    # ------------------------------------------
    # Remove "/ Ecke ..."
    # ------------------------------------------
    adresse = re.sub(r"\s*/\s*Ecke.*", "", adresse).strip()

    # ------------------------------------------
    # Add missing PLZ if not present
    # ------------------------------------------
    if not re.match(r"^\d{5}\s", adresse):
        adresse = f"12489 Berlin {adresse}"

    # ==================================================
    # SPLITTING CASES
    # ==================================================

    # ------------------------------------------
    # Case 1: Multiple addresses separated by ";"
    # ------------------------------------------
    if ";" in adresse:
        parts = [part.strip() for part in adresse.split(";")]
        new_rows = []

        for part in parts:
            if not re.match(r"^\d{5}\s", part):
                part = f"12489 Berlin {part}"

            new_row = row.copy()
            new_row["Adresse"] = part
            new_rows.append(new_row)

        return new_rows

    # ------------------------------------------
    # Case 2: Number range "2 - 4"
    # ------------------------------------------
    range_match = re.search(r"(\d+)\s*-\s*(\d+)", adresse)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        prefix = adresse[:range_match.start()].strip()

        return [
            create_row(row, f"{prefix} {n}")
            for n in range(start, end + 1)
        ]

    # ------------------------------------------
    # Case 3: "16 und 18"
    # ------------------------------------------
    und_match = re.search(r"(\d+)\s*und\s*(\d+)", adresse)
    if und_match:
        num1, num2 = und_match.groups()
        prefix = adresse[:und_match.start()].strip()

        return [
            create_row(row, f"{prefix} {num1}"),
            create_row(row, f"{prefix} {num2}")
        ]

    # ------------------------------------------
    # Case 4: "14/16"
    # ------------------------------------------
    slash_match = re.search(r"(\d+)\s*/\s*(\d+)", adresse)
    if slash_match:
        num1, num2 = slash_match.groups()
        prefix = adresse[:slash_match.start()].strip()

        return [
            create_row(row, f"{prefix} {num1}"),
            create_row(row, f"{prefix} {num2}")
        ]

    # ------------------------------------------
    # Case 5: "73 A-E"
    # ------------------------------------------
    letter_range_match = re.search(r"(\d+)\s*([A-Z])\s*-\s*([A-Z])", adresse)
    if letter_range_match:
        number, start_letter, end_letter = letter_range_match.groups()
        prefix = adresse[:letter_range_match.start()].strip()

        letters = list(string.ascii_uppercase)
        start_index = letters.index(start_letter)
        end_index = letters.index(end_letter)

        return [
            create_row(row, f"{prefix} {number} {letter}")
            for letter in letters[start_index:end_index + 1]
        ]

    # ------------------------------------------
    # Default case
    # ------------------------------------------
    row["Adresse"] = adresse
    return [row]


def create_row(original_row, new_address):
    new_row = original_row.copy()
    new_row["Adresse"] = new_address
    return new_row


# --------------------------------------------------
# 3. Apply transformation
# --------------------------------------------------

processed_rows = []

for _, row in df.iterrows():
    expanded = clean_and_expand_adresse(row)
    processed_rows.extend(expanded)

df = pd.DataFrame(processed_rows).reset_index(drop=True)

df.to_csv(results_path, index=False)

# --------------------------------------------------
# 4. Result
# --------------------------------------------------

print(df.head())