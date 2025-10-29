# src/etl_extraccion.py
import pandas as pd

def extraer_datasets_from_path(path):
    """
    Detecta automáticamente si el archivo es CSV o Excel y lo carga en un DataFrame.
    """
    try:
        if path.endswith(".xlsx") or path.endswith(".xls"):
            df = pd.read_excel(path)  # Leer Excel
        elif path.endswith(".csv"):
            df = pd.read_csv(path, encoding="utf-8", errors="replace")  # Leer CSV
        else:
            raise ValueError("Formato de archivo no soportado. Solo CSV o Excel.")
        return df
    except Exception as e:
        print(f"Error al leer el archivo {path}: {e}")
        raise
