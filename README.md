# 📊 epidemiologia-pdf-consolidado

Este proyecto extrae, limpia y consolida tablas epidemiológicas publicadas en reportes semanales en formato PDF, validado inicialmente para enfermedades mentales como Alzheimer, Depresión y Parkinson en México.

El script localiza automáticamente la página relevante en cada PDF, extrae la tabla con datos por entidad federativa y enfermedad, normaliza los valores numéricos y genera un único archivo CSV listo para análisis.

Los datos provienen de reportes oficiales de vigilancia epidemiológica (SINAVE / Secretaría de Salud) publicados en PDF.

## 📂 Estructura del proyecto

- `src/` contiene el código del proyecto:
  - `pipeline.py`: lógica principal (detección de página, extracción, limpieza, reshape y consolidación).
  - `gui.py`: interfaz gráfica (selección de carpetas, keywords y ejecución).
  - `extraer_tabla.py`: script original por línea de comandos (opcional / legado).
- `data/raw/` contiene los PDFs originales.
- `data/processed/` contiene el archivo consolidado generado.

## 🖥️ Ejecutar con GUI (recomendado)

Desde la raíz del proyecto:

```bash
python -m src.gui
```

La GUI permite:
- Seleccionar carpeta de entrada (PDFs)
- Seleccionar carpeta de salida
- Definir KEYWORDS (enfermedades a buscar y extraer)
- Activar/desactivar guardado de la página donde se encontró la tabla (matched page)

## ⌨️ Ejecutar sin GUI (línea de comandos)

Coloca los PDFs en la carpeta `data/raw` y ejecuta:

```bash
python src/extraer_tabla.py
```

## 📦 Salidas generadas

- `consolidado.csv`: archivo final consolidado.
- `matched_pages/`: carpeta con PDFs de 1 página (solo si se activa la opción en la GUI).