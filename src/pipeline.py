import os
import re
import camelot
import pandas as pd
from pypdf import PdfReader, PdfWriter

SEMANA_REGEX = re.compile(r"Semana\s+(\d{1,2}).*?(\d{4})", re.IGNORECASE)

#COLUMN_MAP = {
#    "Depresión": {"total": 1, "hombres": 2, "mujeres": 3, "total_prev": 4},
#    "Parkinson": {"total": 5, "hombres": 6, "mujeres": 7, "total_prev": 8},
#    "Alzheimer": {"total": 9, "hombres": 10, "mujeres": 11, "total_prev": 12},
#}


def build_column_map(keywords: list[str], start_col: int = 1, step: int = 4):
    col_map = {}
    for i, disease in enumerate(keywords):
        base = start_col + i * step
        col_map[disease] = {
            "total": base,
            "hombres": base + 1,
            "mujeres": base + 2,
            "total_prev": base + 3,
        }
    return col_map



def find_page_and_week(pdf_path: str, keywords: list[str]):
    reader = PdfReader(pdf_path)
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        if all(k.lower() in text.lower() for k in keywords):
            match = SEMANA_REGEX.search(text)
            if match:
                week, year = match.groups()
                return i, int(year), int(week)
    return None, None, None

def extract_matched_page(pdf_path: str, page_index_0: int, out_pdf_path: str):
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    writer.add_page(reader.pages[page_index_0])
    with open(out_pdf_path, "wb") as f:
        writer.write(f)

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = range(df.shape[1])
    df = df[df[0].astype(str).str.strip().ne("")]
    df = df[~df[0].astype(str).str.contains(r"^\s*fuente", case=False, na=False)]
    df = df[~df[0].astype(str).str.contains("Total", case=False, na=False)]
    return df.reset_index(drop=True)

def normalize_number(x):
    if pd.isna(x):
        return pd.NA
    x = str(x).strip()
    if x in ("-", ""):
        return pd.NA
    return int(x.replace(" ", ""))

def reshape(df: pd.DataFrame, year: int, week: int, col_map: dict) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        estado = row[0]
        for disease, cols in col_map.items():
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

def run_pipeline(input_dir: str, output_dir: str, keywords: list[str], save_matched_pages: bool, log_fn=print):
    if not os.path.isdir(input_dir):
        raise ValueError("Input dir inválido.")
    if not os.path.isdir(output_dir):
        raise ValueError("Output dir inválido.")
    if not keywords:
        raise ValueError("KEYWORDS vacías.")

        
    output_csv = os.path.join(output_dir, "consolidado.csv")
    pages_dir = os.path.join(output_dir, "matched_pages")
    if save_matched_pages:
        os.makedirs(pages_dir, exist_ok=True)

    pdf_files = sorted(f for f in os.listdir(input_dir) if f.lower().endswith(".pdf"))
    total = len(pdf_files)
    log_fn(f"PDFs detectados: {total}")

    col_map = build_column_map(keywords)

    all_rows = []
    page_found = 0

    for idx, fname in enumerate(pdf_files, start=1):
        log_fn(f"\n[{idx}/{total}] {fname}")
        pdf_path = os.path.join(input_dir, fname)

        page0, year, week = find_page_and_week(pdf_path, keywords)
        if page0 is None:
            log_fn("  ✗ No se encontró página válida")
            continue

        page_found += 1
        log_fn(f"  ✓ Página {page0 + 1} | Año {year} | Semana {week:02d}")

        if save_matched_pages:
            out_pdf = os.path.join(pages_dir, f"{os.path.splitext(fname)[0]}_p{page0+1}.pdf")
            extract_matched_page(pdf_path, page0, out_pdf)
            log_fn(f"  ✓ Página guardada: matched_pages/{os.path.basename(out_pdf)}")

        tables = camelot.read_pdf(pdf_path, pages=str(page0 + 1), flavor="stream")
        if tables.n == 0:
            log_fn("  ✗ Camelot no detectó tablas")
            continue

        best = max((t.df for t in tables), key=lambda d: d.shape[1])
        df_clean = clean_df(best)
        df_long = reshape(df_clean, year, week, col_map)
        all_rows.append(df_long)

        log_fn(f"  ✓ Filas base: {len(df_clean)} → Filas finales: {len(df_long)}")

    log_fn("\n=== Resumen ===")
    log_fn(f"PDFs procesados: {total}")
    log_fn(f"PDFs con página válida: {page_found}")

    if not all_rows:
        log_fn("No se generaron datos. Archivo final no creado.")
        return

    final_df = pd.concat(all_rows, ignore_index=True)
    final_df.to_csv(output_csv, index=False)
    log_fn(f"\nArchivo final generado: {output_csv}")
    log_fn(f"Total de filas: {len(final_df)}")