print(f"Importando librerías")
import os
import re
import camelot
import pandas as pd
from pypdf import PdfReader

print(f"Iniciando script...")

INPUT_DIR = "data/raw"
OUTPUT_FILE = "data/processed/consolidado.csv"

KEYWORDS = ["Depresión", "Parkinson", "Alzheimer"]
SEMANA_REGEX = re.compile(r"Semana\s+(\d{1,2}).*?(\d{4})", re.IGNORECASE)

COLUMN_MAP = {
    "Depresión": {"total": 1, "hombres": 2, "mujeres": 3, "total_prev": 4},
    "Parkinson": {"total": 5, "hombres": 6, "mujeres": 7, "total_prev": 8},
    "Alzheimer": {"total": 9, "hombres": 10, "mujeres": 11, "total_prev": 12},
}

def find_page_and_week(pdf_path):
    reader = PdfReader(pdf_path)

    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        if all(k.lower() in text.lower() for k in KEYWORDS):
            match = SEMANA_REGEX.search(text)
            if match:
                week, year = match.groups()
                return i + 1, int(year), int(week)

    return None, None, None

def clean_df(df):
    df = df.copy()
    df.columns = range(df.shape[1])

    # filas vacías
    df = df[df[0].astype(str).str.strip().ne("")]

    # quitar totales y notas al pie
    df = df[~df[0].astype(str).str.contains("Total", case=False, na=False)]
    df = df[~df[0].astype(str).str.startswith("FUENTE", na=False)]

    return df.reset_index(drop=True)

def normalize_number(x):
    if pd.isna(x):
        return pd.NA
    x = str(x).strip()
    if x == "-" or x == "":
        return pd.NA
    return int(x.replace(" ", ""))

def reshape(df, year, week):
    records = []

    for _, row in df.iterrows():
        estado = row[0]

        for disease, cols in COLUMN_MAP.items():
            records.append({
                "anio": year,
                "semana": f"{week:02d}",
                "estado": estado,
                "enfermedad": disease,
                "casos_semana": normalize_number(row[cols["total"]]),
                "acumulado_hombres": normalize_number(row[cols["hombres"]]),
                "acumulado_mujeres": normalize_number(row[cols["mujeres"]]),
                "acumulado_anio_anterior": normalize_number(row[cols["total_prev"]]),
            })

    return pd.DataFrame(records)

def main():
    pdf_files = sorted([f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".pdf")])
    total_pdfs = len(pdf_files)

    print(f"=== Inicio ===")
    print(f"PDFs detectados: {total_pdfs}")

    all_rows = []
    page_found = 0

    for idx, file in enumerate(pdf_files, start=1):
        pct = (idx / total_pdfs * 100) if total_pdfs else 100.0
        print(f"\n[{idx}/{total_pdfs}] {pct:5.1f}% → {file}")

        pdf_path = os.path.join(INPUT_DIR, file)

        print("  → Buscando página del reporte")
        page, year, week = find_page_and_week(pdf_path)
        if not page:
            print("  ✗ No se encontró página válida")
            continue

        page_found += 1
        print(f"  ✓ Página {page} | Año {year} | Semana {week:02d}")

        print(f"  → Leyendo tabla con Camelot (página {page})...")
        tables = camelot.read_pdf(pdf_path, pages=str(page), flavor="stream")
        if tables.n == 0:
            print("  ✗ Camelot no detectó tablas")
            continue

        df_raw = tables[0].df
        df_clean = clean_df(df_raw)

        df_long = reshape(df_clean, year, week)
        all_rows.append(df_long)

        print(f"  ✓ Filas base: {len(df_clean)} → Filas finales: {len(df_long)}")

    print("\n=== Resumen ===")
    print(f"PDFs procesados: {total_pdfs}")
    print(f"PDFs con página válida: {page_found}")

    if not all_rows:
        print("No se generaron datos. Archivo final no creado.")
        return

    final_df = pd.concat(all_rows, ignore_index=True)
    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Archivo final generado: {OUTPUT_FILE}")
    print(f"Total de filas: {len(final_df)}")
    print("=== Fin ===")

if __name__ == "__main__":
    main()