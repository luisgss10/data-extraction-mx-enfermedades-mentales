import os
import re
import camelot
import pandas as pd
from pypdf import PdfReader, PdfWriter

SEMANA_REGEX = re.compile(
    r"Semana\s+(\d{1,2}).*?(\d{4})",
    re.IGNORECASE
)
SEMANA_REGEX_2 = re.compile(
    r"semana\s+epidemiol[oó]gica\s+(\d{1,2})\s+del\s+(\d{4})",
    re.IGNORECASE
)

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



def find_page_and_week(pdf_path, KEYWORDS):
    """
    Busca la página del PDF que contiene todas las keywords
    y extrae el año y la semana epidemiológica.
    """
    reader = PdfReader(pdf_path)
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        # Verifica que todas las palabras clave estén presentes
        if all(k.lower() in text.lower() for k in KEYWORDS):
            match = SEMANA_REGEX.search(text) # Opción 1: "Semana 12 2024"
            if match:
                week, year = match.groups()
                return i + 1, int(year), int(week)
            match2 = SEMANA_REGEX_2.search(text) # Opción 2: "semana epidemiológica 42 del 2024"
            if match2:
                week2, year2 = match2.groups()
                return i + 1, int(year2), int(week2) + 1
            # Si encontró keywords pero no pudo sacar semana/año, dar valores de error 8888 y 99
            return i + 1, 8888, 99
    return None, None, None

def extract_matched_page(pdf_path: str, page_index_0: int, out_pdf_path: str):
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    writer.add_page(reader.pages[page_index_0])
    with open(out_pdf_path, "wb") as f:
        writer.write(f)

def clean_df(df, min_numeric_cells=2):
    """
    Limpia la tabla extraída por Camelot dejando solo filas que parecen "estado + datos".

    Regla:
    - Conserva filas donde la columna 0 tiene texto (nombre del estado)
    - y donde existan al menos `min_numeric_cells` valores numéricos enteros en columnas 1..N
    """
    df = df.copy()

    # Renombra columnas con índices numéricos
    df.columns = range(df.shape[1])

    # Normaliza texto en columna 0 (estado / etiquetas)
    df[0] = df[0].astype(str).str.strip()

    # Elimina filas donde la primera columna está vacía
    df = df[df[0].ne("")]

    # Elimina filas obvias de encabezado / pie
    df = df[~df[0].str.match(r"^(ENTIDAD|FEDERATIVA|TOTAL|FUENTE|NOTA)$", case=False, na=False)]

    # Toma todas las columnas excepto la 0 (las numéricas)
    num_cols = [c for c in df.columns if c != 0]

    # Normaliza celdas: quita espacios de millares y convierte "-" en vacío
    cells = df[num_cols].astype(str)
    cells = cells.replace(r"\s+", "", regex=True)
    cells = cells.replace("-", "0", regex=False)

    # Cuenta cuántas celdas son enteros válidos por fila
    is_int = cells.apply(lambda s: s.str.fullmatch(r"\d+").fillna(False))
    numeric_count = is_int.sum(axis=1)

    # Conserva solo filas con suficientes números
    df = df[numeric_count >= min_numeric_cells]

    return df.reset_index(drop=True)

def normalize_number(x):
    if pd.isna(x):
        return 0
    x = str(x).strip()
    if x in ("-", "", ","):
        return 0
    x = x.replace(" ", "").replace(",", "")
    if x == "":
        return 0
    return int(x)

def reshape(df: pd.DataFrame, year: int, week: int, col_map: dict) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        estado = row[0]
        for disease, cols in col_map.items():
            records.append({
                "Anio": year,
                "Semana": f"{week:02d}",
                "Entidad": estado,
                "Padecimiento": disease,
                "Casos_semana": normalize_number(row[cols["total"]]),
                "Acumulado_hombres": normalize_number(row[cols["hombres"]]),
                "Acumulado_mujeres": normalize_number(row[cols["mujeres"]]),
                "Acumulado_anio_anterior": normalize_number(row[cols["total_prev"]]),
            })
    return pd.DataFrame(records)

def print_run_summary(run_log, log_fn=print):
    headers = ["Nombre del archivo", "Anio", "Semana", "Pagina match", "Filas"]
    rows = []

    for r in run_log:
        rows.append([
            str(r.get("file", "")),
            "" if r.get("year") is None else str(r.get("year")),
            "" if r.get("week") is None else f"{int(r.get('week')):02d}",
            "" if r.get("page") is None else str(r.get("page")),
            "" if r.get("rows") is None else str(r.get("rows")),
        ])

    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    def fmt(row):
        return " | ".join(val.ljust(widths[i]) for i, val in enumerate(row))

    log_fn(fmt(headers))
    log_fn("-" * (sum(widths) + 3 * (len(headers) - 1)))

    for row in rows:
        log_fn(fmt(row))

    total = len(run_log)
    ok = sum(1 for r in run_log if (r.get("page") is not None) and (r.get("rows") == 32))
    pct = (ok / total * 100) if total else 0.0
    log_fn(f"\nExito: {ok}/{total} = {pct:.1f}% (match y 32 filas)")




def run_pipeline(input_dir, output_dir, keywords, save_matched_pages=False, log_fn=print, on_file=None):

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
    total_pdfs = len(pdf_files)

    log_fn(f"PDFs detectados: {total_pdfs}")

    col_map = build_column_map(keywords)

    all_rows = []
    page_found = 0
    run_log = []

    for idx, file in enumerate(pdf_files, start=1):
        if on_file:
            on_file(file)
        pct = (idx / total_pdfs * 100) if total_pdfs else 100.0
        pdf_path = os.path.join(input_dir, file)

        page, year, week = None, None, None
        filas_base = None
        status = "‼️"

        page, year, week = find_page_and_week(pdf_path, keywords)

        if not page:
            log_fn("  ‼️ No se encontró página válida")
            run_log.append({"file": file, "year": year, "week": week, "page": page, "rows": filas_base})
            log_fn(f"{idx:>3}/{total_pdfs:<3} | {pct:>6.1f}% | {file} | - | - | {status}")
            continue

        page_found += 1

        if save_matched_pages:
            out_pdf = os.path.join(pages_dir, f"{os.path.splitext(file)[0]}_p{page}.pdf")
            extract_matched_page(pdf_path, page - 1, out_pdf)

        tables = camelot.read_pdf(pdf_path, pages=str(page), flavor="stream")

        if tables.n == 0:
            log_fn("  ⚠️ Camelot no detectó tablas")
            status = "⚠️"
            run_log.append({"file": file, "year": year, "week": week, "page": page, "rows": filas_base})
            log_fn(f"{idx:>3}/{total_pdfs:<3} | {pct:>6.1f}% | {file} | p{page} | {year} W{week:02d} | sin tabla {status} ")
            continue

        df_raw = tables[0].df
        df_clean = clean_df(df_raw)
        filas_base = len(df_clean)
        status = "✅" if filas_base == 32 else "⚠️"

        df_long = reshape(df_clean, year, week, col_map)
        all_rows.append(df_long)
        run_log.append({"file": file, "year": year, "week": week, "page": page, "rows": filas_base})

        log_fn(f"{idx:>3}/{total_pdfs:<3} | {pct:>6.1f}% | {file} | p{page} | {year} W{week:02d} | filas={filas_base} {status}")

    log_fn("\n=== Resumen ===")
    log_fn(f"PDFs procesados: {total_pdfs}")
    log_fn(f"PDFs con página válida: {page_found}")
    log_fn("\n=== Resumen por archivo ===")
    print_run_summary(run_log, log_fn=log_fn)

    if not all_rows:
        log_fn("No se generaron datos. Archivo final no creado.")
        return

    final_df = pd.concat(all_rows, ignore_index=True)
    final_df.to_csv(output_csv, index=False)

    log_fn(f"Archivo final generado: {output_csv}")
    log_fn(f"Total de filas: {len(final_df)}")