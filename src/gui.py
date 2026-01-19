import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.pipeline import run_pipeline


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Extractor de Datos: Boletín Epidemiológico de México")
        self.geometry("900x600")

        self.input_dir = tk.StringVar(value=os.getcwd())
        self.output_dir = tk.StringVar(value=os.getcwd())
        self.keywords = tk.StringVar(value="Depresión, Parkinson, Alzheimer")
        self.save_pages = tk.BooleanVar(value=False)

        self._build_ui()
        self.lift()
        self.attributes("-topmost", True)
        self.after(200, lambda: self.attributes("-topmost", False))
        self.focus_force()

    def _show_keywords_help(self):
        top = tk.Toplevel(self)
        top.title("Ayuda: KEYWORDS")
        top.geometry("600x350")

        txt = tk.Text(top, wrap="word")
        txt.pack(fill="both", expand=True, padx=10, pady=10)
        texto_ayuda="""KEYWORDS
- Escribe las enfermedades a buscar dentro del PDF.
- Se usan para encontrar la página donde está la tabla.
- Separa cada una con coma.
- Deben ir en el mismo orden en que aparecen en la tabla del boletín.
- Agrega solamente las 2 o 3 enfermedades, tal cual están en el encabezado de la tabla deseada.
- Ejemplo: Depresión, Parkinson, Alzheimer)."""
        txt.insert("1.0", texto_ayuda)
        txt.config(state="disabled")

        ttk.Button(top, text="Cerrar", command=top.destroy).pack(pady=(0, 10))

    
    
    def _build_ui(self):
        pad = 10
        frm = ttk.Frame(self)
        frm.pack(fill="x", padx=pad, pady=pad)

        ttk.Label(frm, text="Carpeta de entrada (PDFs):").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.input_dir, width=70).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(frm, text="Browse", command=self._browse_input).grid(row=0, column=2)

        ttk.Label(frm, text="Carpeta de salida:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(frm, textvariable=self.output_dir, width=70).grid(row=1, column=1, sticky="we", padx=6, pady=(6, 0))
        ttk.Button(frm, text="Browse", command=self._browse_output).grid(row=1, column=2, pady=(6, 0))

        ttk.Label(frm, text="KEYWORDS:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        kw_frame = ttk.Frame(frm)
        kw_frame.grid(row=2, column=1, sticky="we", padx=6, pady=(6, 0))

        ttk.Entry(kw_frame, textvariable=self.keywords).pack(side="left", fill="x", expand=True)

        ttk.Button(kw_frame, text="?", width=3, command=self._show_keywords_help).pack(side="left", padx=(6, 0))
        #ttk.Entry(frm, textvariable=self.keywords, width=70).grid(row=2, column=1, sticky="we", padx=6, pady=(6, 0))

        ttk.Checkbutton(frm, text="Guardar página match", variable=self.save_pages).grid(
            row=3, column=1, sticky="w", pady=(8, 0)
        )
        ttk.Button(frm, text="RUN", command=self._run_clicked).grid(row=3, column=2, sticky="e", pady=(8, 0))

        frm.columnconfigure(1, weight=1)

        self.log = tk.Text(self, height=25, wrap="word")
        self.log.pack(fill="both", expand=True, padx=pad, pady=(0, pad))
        texto_bienvenida = """============================================================
Extractor de tablas: Boletín Epidemiológico (México)
============================================================

Este programa procesa boletines en PDF y genera un CSV consolidado listo para análisis.
El boletín es actualizado semanalmente por el Sistema Nacional de Vigilancia Epidemiológica (SINAVE).
Link: https://www.gob.mx/salud/acciones-y-programas/direccion-general-de-epidemiologia-boletin-epidemiologico

Pasos:
1) Selecciona la carpeta con los PDFs (entrada).
2) Selecciona la carpeta donde se guardarán los resultados (salida).
3) Escribe las KEYWORDS separadas por coma (ej. Depresión, Parkinson, Alzheimer).
   Nota: estas KEYWORDS definen las enfermedades a extraer.
4) (Opcional) Activa “Guardar página match” para guardar la página donde se encontró la tabla.
5) Presiona RUN.

Salida:
- consolidado.csv
- matched_pages/ (si activas la opción)
"""

        self._log(texto_bienvenida)
        footer = ttk.Frame(self)
        footer.pack(fill="x", padx=10, pady=(0, 10))
        texto_acercade="""
Acerca de:
Extractor de tablas - Boletín Epidemiológico SINAVE
Versión: 0.0.1

Este proyecto fue realizado académicamente en colaboración del Tecnológico de Monterrey y el IMSS como parte del curso de Proyecto Integrador de la Maestría en Inteligencia Artificial Aplicada (MNA), periodo Enero-Abril, 2026.

Equipo:
A01795941@tec.mx - Juan Carlos Pérez
A01795838@tec.mx - Javier Rebull 
A01232963@tec.mx - Luis Sánchez
"""
        ttk.Button(
            footer,
            text="Acerca de...",
            command=lambda: messagebox.showinfo("Acerca de", texto_acercade)
        ).pack(side="left")

        ttk.Button(footer, text="Salir", command=self.destroy).pack(side="right")



    def _browse_input(self):
        d = filedialog.askdirectory(initialdir=self.input_dir.get() or os.getcwd())
        if d:
            self.input_dir.set(d)

    def _browse_output(self):
        d = filedialog.askdirectory(initialdir=self.output_dir.get() or os.getcwd())
        if d:
            self.output_dir.set(d)

    
    def _show_csv_preview(self, csv_path: str, n_rows: int = 50):
        import pandas as pd
        from tkinter import ttk
        import tkinter as tk

        if not os.path.exists(csv_path):
            messagebox.showwarning("Preview", f"No se encontró el archivo:\n{csv_path}")
            return

        df = pd.read_csv(csv_path).head(n_rows)

        top = tk.Toplevel(self)
        top.title(f"Preview: {os.path.basename(csv_path)} (primeras {len(df)} filas)")
        top.geometry("1000x500")

        frame = ttk.Frame(top)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        cols = list(df.columns)
        tree = ttk.Treeview(frame, columns=cols, show="headings")
        tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        vsb.pack(side="right", fill="y")
        tree.configure(yscrollcommand=vsb.set)

        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=140, anchor="w")

        for _, row in df.iterrows():
            tree.insert("", "end", values=[row[c] for c in cols])

        ttk.Button(top, text="Cerrar", command=top.destroy).pack(pady=(0, 10))

    
    def _log(self, msg: str):
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.update_idletasks()

    def _run_clicked(self):
        inp = self.input_dir.get().strip()
        out = self.output_dir.get().strip()
        kw = [k.strip() for k in self.keywords.get().split(",") if k.strip()]
        save = bool(self.save_pages.get())

        def worker():
            try:
                self._log("\n=== Inicio ===")
                run_pipeline(inp, out, kw, save, log_fn=self._log)
                self._log("\n=== Fin ===")
                output_csv = os.path.join(out, "consolidado.csv")
                self.after(0, lambda: self._show_csv_preview(output_csv))
            except Exception as e:
                self._log(f"\nERROR: {e}")
                messagebox.showerror("Error", str(e))

        threading.Thread(target=worker, daemon=True).start()

if __name__ == "__main__":
    App().mainloop()