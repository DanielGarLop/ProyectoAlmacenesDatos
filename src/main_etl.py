# main_etl.py
from etl_extraccion import extraer_datasets_from_path
from etl_transformacion import limpiar_dataset, detectar_outliers
from etl_carga import cargar_a_excel
import os

def run_etl():
    print("🔹 Iniciando ETL completo...")

    # Carpeta actual del script
    carpeta_actual = os.path.dirname(os.path.abspath(__file__))

    # Ruta relativa a los datasets (subir un nivel y luego entrar a data/processed)
    ruta_datos = os.path.join(carpeta_actual, "..", "data", "processed")
    ruta_datos = os.path.normpath(ruta_datos)  # limpia la ruta para cualquier OS

    # Verificar que la carpeta exista
    if not os.path.exists(ruta_datos):
        print(f"[ETL] La carpeta '{ruta_datos}' NO existe. Por favor pon tus archivos ahí.")
        return
    else:
        print(f"[ETL] Carpeta encontrada: {ruta_datos}")

    # Extraer todos los datasets de la carpeta
    datasets = extraer_datasets_from_path(ruta_datos)
    if not datasets:
        print("[ETL] No se encontraron datasets en la carpeta.")
        return

    # Procesar cada archivo
    for nombre, df in datasets.items():
        print(f"\n--- Procesando {nombre} ---")
        df_limpio = limpiar_dataset(df)
        outliers = detectar_outliers(df_limpio)
        print(f"[ETL] Outliers detectados: {outliers}")
        cargar_a_excel(df_limpio, nombre)

    print("\n✅ ETL completado exitosamente")

if __name__ == "__main__":
    run_etl()
