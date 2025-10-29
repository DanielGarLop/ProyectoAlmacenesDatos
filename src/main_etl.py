# main_etl.py
from etl_extraccion import extraer_datasets
from etl_transformacion import limpiar_dataset, detectar_outliers
from etl_carga import cargar_a_excel

def run_etl():
    print("🔹 Iniciando ETL completo...")

    datasets = extraer_datasets()
    if datasets is None:
        print("[ETL] Error al cargar datasets.")
        return

    for nombre, df in datasets.items():
        print(f"\n--- Procesando {nombre} ---")
        df_limpio = limpiar_dataset(df)
        outliers = detectar_outliers(df_limpio)
        print(f"[ETL] Outliers detectados: {outliers}")
        cargar_a_excel(df_limpio, nombre)

    print("\n✅ ETL completado exitosamente")

if __name__ == "__main__":
    run_etl()
