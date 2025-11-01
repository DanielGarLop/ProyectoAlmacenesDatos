# etl_carga.py
import os

def cargar_a_excel(df, ruta):
    """
    Guarda un DataFrame limpio en la ruta especificada.
    Crea la carpeta padre si no existe.
    """
    save_dir = os.path.dirname(ruta)
    os.makedirs(save_dir, exist_ok=True)  # Crear carpeta si no existe
    df.to_excel(ruta, index=False)
    print(f"[CARGA] {os.path.basename(ruta)} guardado en {save_dir}")
