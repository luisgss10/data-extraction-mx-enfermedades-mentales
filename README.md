# 📊 Data Extraction MX - Enfermedades Mentales

Pipeline automatizado para extracción, limpieza y consolidación de tablas epidemiológicas desde reportes semanales en PDF del Sistema Nacional de Vigilancia Epidemiológica (SINAVE / Secretaría de Salud de México).

Validado para enfermedades neurológicas y trastornos mentales: **Alzheimer**, **Depresión** y **Parkinson**.

---

## 🎯 Objetivo

Automatizar la extracción de datos epidemiológicos publicados semanalmente en formato PDF, generando datasets estructurados (CSV) listos para análisis predictivo y visualización.

---

## ✨ Características

- **Detección automática** de la página relevante en cada PDF
- **Extracción de tablas** con datos por entidad federativa y enfermedad
- **Normalización** de valores numéricos y limpieza de datos
- **Consolidación** de múltiples PDFs en un único archivo CSV
- **Interfaz gráfica (GUI)** para facilidad de uso
- **CLI** para integración en pipelines automatizados

---

## 📂 Estructura del Proyecto

```
data-extraction-mx-enfermedades-mentales/
├── src/
│   ├── __init__.py
│   ├── pipeline.py        # Lógica principal (detección, extracción, limpieza, consolidación)
│   ├── gui.py             # Interfaz gráfica
│   └── extraer_tabla.py   # Script CLI (legado)
├── data/
│   ├── raw/               # PDFs originales de entrada
│   └── processed/         # Archivos CSV consolidados generados
├── requirements.txt
├── .python-version
└── README.md
```

---

## 🛠️ Requisitos

- **Python 3.12+**
- **Ghostscript** (dependencia del sistema para camelot-py)

---

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/luisgss10/data-extraction-mx-enfermedades-mentales.git
cd data-extraction-mx-enfermedades-mentales
```

### 2. Instalar Ghostscript

**macOS:**
```bash
brew install ghostscript
```

**Ubuntu/Debian:**
```bash
sudo apt-get install ghostscript
```

**Windows:**
Descargar desde [ghostscript.com](https://ghostscript.com/releases/gsdnld.html) y agregar al PATH.

### 3. Crear ambiente virtual

```bash
python3.12 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 4. Instalar dependencias

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 🖥️ Uso

### Opción 1: Interfaz Gráfica (Recomendado)

```bash
python -m src.gui
```

La GUI permite:
- Seleccionar carpeta de entrada (PDFs)
- Seleccionar carpeta de salida
- Definir keywords (enfermedades a buscar)
- Activar/desactivar guardado de páginas extraídas

### Opción 2: Línea de Comandos

Colocar los PDFs en `data/raw/` y ejecutar:

```bash
python -m src.extraer_tabla
```

---

## 📦 Salidas Generadas

| Archivo | Descripción |
|---------|-------------|
| `consolidado.csv` | Dataset final con todos los datos extraídos |
| `matched_pages/` | PDFs de 1 página con las tablas encontradas (opcional) |

---

## 🔧 Dependencias Principales

| Paquete | Uso |
|---------|-----|
| `camelot-py` | Extracción de tablas desde PDF |
| `pandas` | Manipulación y limpieza de datos |
| `opencv-python` | Procesamiento de imágenes para detección |
| `ghostscript` | Backend para renderizado de PDF |
| `pypdf` | Manipulación de archivos PDF |

---

## 📊 Fuente de Datos

Los datos provienen de los **Boletines Epidemiológicos Semanales** publicados por:
- Sistema Nacional de Vigilancia Epidemiológica (SINAVE)
- Secretaría de Salud de México

---

## 👥 Equipo

Proyecto desarrollado como parte del capstone del MNA en Inteligencia Artificial Aplicada (Tecnológico de Monterrey) en colaboración con el IMSS.

---

## 📄 Licencia

MIT License - Ver archivo `LICENSE` para más detalles.