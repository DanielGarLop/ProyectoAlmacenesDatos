# etl_carga.py
import os

def cargar_a_excel(df, nombre):
    """Guarda un DataFrame limpio en data/processed"""
    os.makedirs("data/processed", exist_ok=True)
    ruta = f"data/processed/{nombre}_limpio.xlsx"
    df.to_excel(ruta, index=False)
    print(f"[CARGA] {nombre} guardado en {ruta}")
