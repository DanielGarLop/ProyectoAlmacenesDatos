import os
import pandas as pd

def extraer_datasets_from_path(path):
    """
    Lee archivos CSV, Excel o JSON en la ruta indicada.
    - Si es un archivo individual, devuelve un DataFrame.
    - Si es una carpeta, devuelve un diccionario {nombre_archivo: DataFrame}.
    """
    if os.path.isfile(path):  # Archivo individual
        return _leer_archivo(path)
    
    elif os.path.isdir(path):  # Carpeta
        datasets = {}
        for archivo in os.listdir(path):
            ruta = os.path.join(path, archivo)
            if archivo.endswith((".csv", ".xlsx", ".xls", ".json")):
                datasets[archivo] = _leer_archivo(ruta)
        if not datasets:
            raise ValueError("No se encontraron archivos CSV, Excel o JSON en la carpeta")
        return datasets
    
    else:
        raise ValueError("Ruta inválida: debe ser un archivo o carpeta existente")

def _leer_archivo(path):
    """
    Lee un archivo individual y devuelve un DataFrame.
    """
    if path.endswith((".xlsx", ".xls")):
        return pd.read_excel(path)
    elif path.endswith(".csv"):
        return pd.read_csv(path, encoding="utf-8", errors="replace")
    elif path.endswith(".json"):
        return pd.read_json(path, encoding="utf-8")
    else:
        raise ValueError("Formato de archivo no soportado")
