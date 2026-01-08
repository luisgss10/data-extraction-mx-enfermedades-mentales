# epidemiologia-pdf-consolidado

Este proyecto extrae, limpia y consolida tablas epidemiológicas publicadas en reportes semanales en formato PDF, esto específicamente para enfermedades mentales como Alzheimer, Depresión y Parkinson en México.

El script localiza automáticamente la página relevante en cada PDF, extrae la tabla con datos por entidad federativa y enfermedad, normaliza los valores numéricos y genera un único archivo CSV listo para análisis.

Los datos provienen de reportes oficiales de vigilancia epidemiológica (SINAVE / Secretaría de Salud) publicados en PDF.

## Estructura del proyecto

src/ contiene el script de extracción y consolidación.  
data/raw/ contiene los PDFs originales.  
data/processed/ contiene el archivo consolidado generado.

## Generación del archivo consolidado

Coloca los PDFs en la carpeta `data/raw` y ejecuta:

```bash
python src/extraer_tabla.py