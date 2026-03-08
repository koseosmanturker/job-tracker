import argparse
import unicodedata
from pathlib import Path

from repository import read_jobs_csv, write_jobs_csv


# Normalizes free text for robust matching across case/diacritic/encoding issues.
# This lets interactive lookups match CSV content even with inconsistent input.
def normalize_text(value: str) -> str:
    text = (value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    # Common TR letter fallback for easier matching
    replacements = {
        "ş": "s",
        "ı": "i",
        "ğ": "g",
        "ü": "u",
        "ö": "o",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return " ".join(text.split())


# Finds best matching row key for given company and title inputs.
# First tries exact normalized match, then falls back to relaxed contains match.
def find_match(jobs: dict, company_input: str, title_input: str):
    company_norm = normalize_text(company_input)
    title_norm = normalize_text(title_input)

    # 1) exact normalized match
    for key, row in jobs.items():
        if (
            normalize_text(row.get("company", "")) == company_norm
            and normalize_text(row.get("job_title", "")) == title_norm
        ):
            return key

    # 2) relaxed contains match
    for key, row in jobs.items():
        if (
            company_norm in normalize_text(row.get("company", ""))
            and title_norm in normalize_text(row.get("job_title", ""))
        ):
            return key
    return None


# Prints limited same-company suggestions when exact job cannot be found.
# This helps user quickly discover the right title spelling from current CSV.
def print_suggestions(jobs: dict, company_input: str, limit: int = 8):
    company_norm = normalize_text(company_input)
    suggestions = []
    for row in jobs.values():
        c = normalize_text(row.get("company", ""))
        if company_norm and company_norm in c:
            suggestions.append((row.get("company", ""), row.get("job_title", "")))
    if not suggestions:
        return
    print("\nSirkete gore olasi kayitlar:")
    for company, title in suggestions[:limit]:
        print(f"- {company} | {title}")


# Runs interactive CLI flow to set "downloaded=True" for selected job row.
# It repeatedly asks for company/title and persists updates to CSV.
def main():
    parser = argparse.ArgumentParser(
        description="Interaktif olarak company + job_title ile kaydi bulup downloaded=True yapar."
    )
    parser.add_argument("--csv", default="jobs.csv", help="CSV dosya yolu (default: jobs.csv)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    print("Cikis icin herhangi bir adimda QUIT yaz.")

    while True:
        jobs = read_jobs_csv(str(csv_path))
        if not jobs:
            print("Kayit bulunamadi.")
            return

        company_input = input("\nSirket adi: ").strip()
        if normalize_text(company_input) == "quit":
            print("Cikiliyor.")
            return

        title_input = input("Pozisyon adi: ").strip()
        if normalize_text(title_input) == "quit":
            print("Cikiliyor.")
            return

        if not company_input or not title_input:
            print("Sirket ve pozisyon bos olamaz.")
            continue

        matched_key = find_match(jobs, company_input, title_input)
        if not matched_key:
            print("Eslesen kayit bulunamadi.")
            print_suggestions(jobs, company_input)
            continue

        jobs[matched_key]["downloaded"] = True
        write_jobs_csv(str(csv_path), jobs)
        row = jobs[matched_key]
        print("Guncellendi:")
        print(f"company   : {row['company']}")
        print(f"job_title : {row['job_title']}")
        print(f"downloaded: {row['downloaded']}")


if __name__ == "__main__":
    main()
