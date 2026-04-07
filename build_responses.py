"""
build_responses.py
------------------
Copies existing SuperWEB2 CSVs into csv_responses/ with standardised names,
then converts each CSV to a clean Django-friendly JSON in json_responses/.

Run once:  python build_responses.py
"""

import csv
import json
import os
import shutil
import io

BASE = os.path.dirname(os.path.abspath(__file__))
CSV_OUT = os.path.join(BASE, "csv_responses")
JSON_OUT = os.path.join(BASE, "json_responses")

# ---------------------------------------------------------------------------
# Mapping: target filename  →  existing source path (relative to BASE)
# Files marked None are not yet downloaded; a placeholder CSV is created.
# ---------------------------------------------------------------------------
MAPPING = {
    "sex.csv":                      "Demography/Ward 2022 by Sex.csv",
    "age_group.csv":                "Demography/Age group.csv",
    "population_group.csv":         "Demography/Population group.csv",
    "sector_type.csv":              None,   # not yet downloaded
    "household_size.csv":           "Dwellings/csv/Household size.csv",
    "age_of_hh_head.csv":           "Social/Csv/Age of head of the HOUSE.csv",
    "sex_of_hh_head.csv":           "Social/Csv/Sex of head of the household.csv",
    "employment_status.csv":        "Demography/Official employment status.csv",
    "main_dwelling.csv":            "Dwellings/csv/Type of main dwelling - main.csv",
    "tenure_status.csv":            "Dwellings/csv/Tenure status.csv",
    "piped_water.csv":              "Infrastructure/Piped water.csv",
    "water_source.csv":             "Infrastructure/Source of water.csv",
    "toilet_facilities.csv":        "Infrastructure/Toilet facilities.csv",
    "refuse_removal.csv":           "Infrastructure/Refuse or rubbish.csv",
    "energy_lighting.csv":          "Infrastructure/Energy or fuel for lighting.csv",
    "energy_cooking.csv":           None,   # not yet downloaded
    "energy_heating.csv":           None,   # not yet downloaded
    "disability_seeing.csv":        "Disability_Data_Set/Seeing.csv",
    "disability_hearing.csv":       "Disability_Data_Set/Hearing.csv",
    "disability_remembering.csv":   None,   # not yet downloaded
    "disability_wheelchair.csv":    "Disability_Data_Set/Assistive devices and medication-A wheelchair.csv",
    "disability_walking_stick.csv": "Disability_Data_Set/Assistive devices and medication-Walking stick or frame.csv",
    "disability_chronic_medication.csv": "Disability_Data_Set/Assistive devices and medication-Chronic medication.csv",
    "education_level.csv":          "Education/Csv/Highest level of education.csv",
    "educational_institution.csv":  "Education/Csv/Educational institution.csv",
    "household_income.csv":         "Dwellings/csv/Household class of income.csv",
}

PLACEHOLDER_TEMPLATE = """SuperWEB2(tm)

"PLACEHOLDER – file not yet downloaded"
"{label}"
"Counting: pending"

Filters:
"Default Summation","pending"

"""


def _csv_rows(text):
    """Return all parsed CSV rows from raw text, skipping blank lines."""
    rows = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            reader = csv.reader(io.StringIO(stripped))
            row = [c.strip() for c in next(reader)]
            rows.append(row)
        except Exception:
            continue
    return rows


def _is_numeric(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False


def _extract_meta(rows, header_idx):
    """Return (title, subtitle, measure) from rows before header_idx."""
    meta_texts = []
    skip = {"superweb", "filter", "default", "counting"}
    for r in rows[:header_idx]:
        text = " ".join(r).strip()
        if text and not any(text.lower().startswith(s) for s in skip):
            meta_texts.append(text)
    title    = meta_texts[0] if len(meta_texts) > 0 else ""
    subtitle = meta_texts[1] if len(meta_texts) > 1 else ""
    measure  = meta_texts[2] if len(meta_texts) > 2 else ""
    return title, subtitle, measure


def _parse_wide(rows):
    """
    Wide format: categories are columns.

      Header:  "Category Dim","Cat A","Cat B","Total",
      Geo row: "Ward 2022",
      Data:    "52502001",1.0,2.0,3.0,

    Returns categories list and data dict.
    """
    # Find first data row: first cell looks like a ward code (starts with digit,
    # 5+ chars) AND second cell is numeric
    import re
    ward_pattern = re.compile(r'^\d{5}')
    data_start = None
    for idx, row in enumerate(rows):
        if len(row) >= 2 and ward_pattern.match(row[0]) and _is_numeric(row[1]):
            data_start = idx
            break
    if data_start is None or data_start < 2:
        return "", "", "", [], {}

    header_row = rows[data_start - 2]
    categories = [c for c in header_row[1:] if c]
    title, subtitle, measure = _extract_meta(rows, data_start - 2)

    data = {}
    for row in rows[data_start:]:
        if not row or row[0].lower().startswith("(c)"):
            break
        ward_key = row[0].split(":")[0].strip()
        if not ward_key:
            continue
        values = {}
        for j, cat in enumerate(categories):
            cell = row[j + 1] if j + 1 < len(row) else ""
            if cell == "" or cell.lower() in ("n/a", "-"):
                values[cat] = None
            else:
                try:
                    values[cat] = round(float(cell), 6)
                except ValueError:
                    values[cat] = cell
        data[ward_key] = values

    cats_no_total = [c for c in categories if c.lower() != "total"]
    return title, subtitle, measure, cats_no_total, data


def _parse_long(rows):
    """
    Long (tall) format: one row per (ward, category) pair.

      Header:  "Summation Options","Geography 2016","Category Column","Count",
      Data:    "measure","52502001 : Ward 1","grade 0",323.17678

    Returns categories list and data dict grouped by ward.
    """
    # Find header row: contains "Geography" and "Count"
    header_idx = None
    for idx, row in enumerate(rows):
        lower = [c.lower() for c in row]
        if any("geography" in c or "ward" in c for c in lower) and \
           any("count" in c for c in lower):
            header_idx = idx
            break
    if header_idx is None:
        return "", "", "", [], {}

    header_row = rows[header_idx]
    # Identify column positions
    lower_h = [c.lower() for c in header_row]
    geo_col = next((i for i, c in enumerate(lower_h)
                    if "geography" in c or "ward" in c), 1)
    cat_col = next((i for i, c in enumerate(lower_h)
                    if "level" in c or "institution" in c or "income" in c
                    or "size" in c or "status" in c
                    or (i > geo_col and "count" not in c and c)), geo_col + 1)
    count_col = next((i for i, c in enumerate(lower_h) if "count" in c),
                     len(header_row) - 1)

    title, subtitle, measure = _extract_meta(rows, header_idx)

    data = {}
    categories_seen = []
    for row in rows[header_idx + 1:]:
        if not row or row[0].lower().startswith("(c)"):
            break
        if len(row) <= max(geo_col, cat_col, count_col):
            continue
        ward_key = row[geo_col].split(":")[0].strip()
        cat_val  = row[cat_col].strip()
        count    = row[count_col].strip()
        if not ward_key or not cat_val:
            continue
        if ward_key not in data:
            data[ward_key] = {}
        try:
            data[ward_key][cat_val] = round(float(count), 6)
        except ValueError:
            data[ward_key][cat_val] = count
        if cat_val not in categories_seen:
            categories_seen.append(cat_val)

    # Build per-ward totals
    for ward_key, vals in data.items():
        nums = [v for v in vals.values() if isinstance(v, (int, float))]
        vals["Total"] = round(sum(nums), 6)

    return title, subtitle, measure, categories_seen, data


def parse_superweb_csv(filepath):
    """
    Parse a SuperWEB2 CSV export (wide or long format).

    Returns a dict:
    {
        "title":       str,
        "subtitle":    str,
        "measure":     str,
        "categories":  [str, ...],
        "data": {
            "52502001": {"Cat A": 1.23, ..., "Total": 5.79},
            ...
            "Total": {...}
        }
    }
    """
    with open(filepath, encoding="utf-8-sig") as fh:
        raw = fh.read()

    rows = _csv_rows(raw)

    # Detect format: long format has a header row whose FIRST cell is
    # "Summation Options" (wide format only has "Default Summation" as a value)
    is_long = any(
        row and "summation options" in row[0].lower()
        for row in rows[:20]
    )

    if is_long:
        title, subtitle, measure, categories, data = _parse_long(rows)
    else:
        title, subtitle, measure, categories, data = _parse_wide(rows)

    return {
        "title":      title,
        "subtitle":   subtitle,
        "measure":    measure,
        "source":     "Statistics South Africa – SuperWEB2",
        "categories": categories,
        "data":       data,
    }


def write_placeholder(dest, label):
    with open(dest, "w", encoding="utf-8") as fh:
        fh.write(PLACEHOLDER_TEMPLATE.format(label=label))
    print(f"  [PLACEHOLDER] {os.path.basename(dest)}")


def main():
    os.makedirs(CSV_OUT, exist_ok=True)
    os.makedirs(JSON_OUT, exist_ok=True)

    for target, source_rel in MAPPING.items():
        csv_dest = os.path.join(CSV_OUT, target)
        json_dest = os.path.join(JSON_OUT, target.replace(".csv", ".json"))

        # ── Step 1: populate csv_responses/ ──────────────────────────────
        if source_rel is None:
            write_placeholder(csv_dest, target.replace(".csv", "").replace("_", " ").title())
            # Write a minimal JSON stub for placeholders
            stub = {
                "title":      target.replace(".csv", "").replace("_", " ").title(),
                "subtitle":   "PLACEHOLDER – not yet downloaded from SuperWEB2",
                "measure":    "",
                "source":     "Statistics South Africa – SuperWEB2",
                "categories": [],
                "data":       {},
            }
            with open(json_dest, "w", encoding="utf-8") as jf:
                json.dump(stub, jf, indent=2)
            print(f"  [STUB JSON]   {os.path.basename(json_dest)}")
            continue

        src = os.path.join(BASE, source_rel)
        if not os.path.exists(src):
            print(f"  [WARNING] Source not found: {source_rel}")
            write_placeholder(csv_dest, target)
            continue

        shutil.copy2(src, csv_dest)
        print(f"  [COPIED]      {source_rel}  →  csv_responses/{target}")

        # ── Step 2: convert to JSON ───────────────────────────────────────
        try:
            result = parse_superweb_csv(csv_dest)
            with open(json_dest, "w", encoding="utf-8") as jf:
                json.dump(result, jf, indent=2, ensure_ascii=False)
            ward_count = len([k for k in result["data"] if k != "Total"])
            print(f"  [JSON]        json_responses/{os.path.basename(json_dest)}  "
                  f"({ward_count} wards, {len(result['categories'])} categories)")
        except Exception as exc:
            print(f"  [ERROR] Could not parse {target}: {exc}")


if __name__ == "__main__":
    print("Building csv_responses/ and json_responses/ …\n")
    main()
    print("\nDone.")
