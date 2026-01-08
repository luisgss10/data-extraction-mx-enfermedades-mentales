# Mensaje inicial para saber que el script arrancó
print("Importando librerías")

# Librerías estándar
import os              # Manejo de archivos y directorios
import re              # Expresiones regulares

# Librerías externas
import camelot         # Extracción de tablas desde PDF
import pandas as pd    # Manipulación de datos
from pypdf import PdfReader  # Lectura de texto en PDFs

print("Iniciando script...")

# Directorio donde están los PDFs de entrada
INPUT_DIR = "data/raw"

# Archivo CSV final consolidado
OUTPUT_FILE = "data/processed/consolidado.csv"

# Palabras clave que deben aparecer juntas en la página correcta
KEYWORDS = ["Depresión", "Parkinson", "Alzheimer"]

# Regex para extraer semana y año del texto del PDF
# Busca textos que contengan la palabra "Semana" seguida de:
# - un número de semana (1 o 2 dígitos) → grupo 1
# - cualquier texto intermedio
# - un año de 4 dígitos → grupo 2
# No distingue entre mayúsculas y minúsculas
# Ejemplo esperado: "Semana 12 2024"
SEMANA_REGEX = re.compile(
    r"Semana\s+(\d{1,2}).*?(\d{4})",
    re.IGNORECASE
)

# Mapeo fijo de columnas según la estructura de la tabla del PDF
COLUMN_MAP = {
    "Depresión": {"total": 1, "hombres": 2, "mujeres": 3, "total_prev": 4},
    "Parkinson": {"total": 5, "hombres": 6, "mujeres": 7, "total_prev": 8},
    "Alzheimer": {"total": 9, "hombres": 10, "mujeres": 11, "total_prev": 12},
}

def find_page_and_week(pdf_path):
    """
    Busca la página del PDF que contiene todas las keywords
    y extrae el año y la semana epidemiológica.
    """
    reader = PdfReader(pdf_path)

    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""

        # Verifica que todas las palabras clave estén presentes
        if all(k.lower() in text.lower() for k in KEYWORDS):
            match = SEMANA_REGEX.search(text)
            if match:
                week, year = match.groups()
                return i + 1, int(year), int(week)

    # Si no se encuentra una página válida
    return None, None, None

def clean_df(df):
    """
    Limpia la tabla extraída por Camelot:
    - Normaliza nombres de columnas
    - Elimina filas vacías
    - Quita totales y notas al pie
    """
    df = df.copy()

    # Renombra columnas con índices numéricos
    df.columns = range(df.shape[1])

    # Elimina filas completamente vacías
    df = df[df[0].astype(str).str.strip().ne("")]

    # Elimina filas de totales
    df = df[~df[0].astype(str).str.contains("Total", case=False, na=False)]

    # Elimina filas de fuente o notas
    df = df[~df[0].astype(str).str.startswith("FUENTE", na=False)]

    return df.reset_index(drop=True)

def normalize_number(x):
    """
    Convierte valores numéricos a entero.
    Maneja guiones, vacíos y NaN.
    """
    if pd.isna(x):
        return pd.NA

    x = str(x).strip()

    if x in ("-", ""):
        return pd.NA

    # Quita espacios antes de convertir
    return int(x.replace(" ", ""))

def reshape(df, year, week):
    """
    Convierte la tabla ancha en formato largo:
    una fila por estado y enfermedad.
    """
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
    """
    Función principal:
    - Recorre PDFs
    - Detecta página correcta
    - Extrae tabla
    - Limpia y normaliza datos
    - Consolida todo en un CSV
    """
    pdf_files = sorted(
        f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".pdf")
    )
    total_pdfs = len(pdf_files)

    print("=== Inicio ===")
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

        print(f"  → Leyendo tabla con Camelot (página {page})")
        tables = camelot.read_pdf(pdf_path, pages=str(page), flavor="stream")

        if tables.n == 0:
            print("  ✗ Camelot no detectó tablas")
            continue

        # Toma la primera tabla detectada
        df_raw = tables[0].df

        # Limpieza de la tabla
        df_clean = clean_df(df_raw)

        # Conversión a formato largo
        df_long = reshape(df_clean, year, week)
        all_rows.append(df_long)

        print(f"  ✓ Filas base: {len(df_clean)} → Filas finales: {len(df_long)}")

    print("\n=== Resumen ===")
    print(f"PDFs procesados: {total_pdfs}")
    print(f"PDFs con página válida: {page_found}")

    if not all_rows:
        print("No se generaron datos. Archivo final no creado.")
        return

    # Concatenación final
    final_df = pd.concat(all_rows, ignore_index=True)

    # Escritura del CSV consolidado
    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Archivo final generado: {OUTPUT_FILE}")
    print(f"Total de filas: {len(final_df)}")
    print("=== Fin ===")

# Punto de entrada del script
if __name__ == "__main__":
    main()
