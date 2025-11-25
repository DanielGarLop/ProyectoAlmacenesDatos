# etl_carga.py
import os
import pandas as pd

def cargar_dataframe(df, ruta, formato="excel"):
    """
    Guarda un DataFrame limpio en la ruta especificada.
    - df: DataFrame a guardar
    - ruta: path completo donde se guardará el archivo
    - formato: "excel" o "csv"
    Crea la carpeta padre si no existe.
    Retorna la ruta final del archivo guardado.
    """
    save_dir = os.path.dirname(ruta)
    os.makedirs(save_dir, exist_ok=True)  # Crear carpeta si no existe

    ext = formato.lower()
    if ext == "excel":
        df.to_excel(ruta, index=False)
    elif ext == "csv":
        df.to_csv(ruta, index=False, encoding="utf-8")
    else:
        raise ValueError("Formato no soportado. Usa 'excel' o 'csv'.")

    print(f"[CARGA] {os.path.basename(ruta)} guardado en {save_dir}")
    return ruta
