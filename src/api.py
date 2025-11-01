# src/api.py
import os
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
import threading

from etl_extraccion import extraer_datasets_from_path
from etl_transformacion import limpiar_dataset, detectar_outliers

# Carpeta base donde se guardarán archivos subidos y resultados
BASE_DIR = "uploaded_files"
os.makedirs(BASE_DIR, exist_ok=True)

# Diccionario para controlar el estado de cada proceso
process_status = {}

app = FastAPI(title="API de ETL de Accidentes de Tráfico")


@app.get("/")
async def root():
    return {"message": "API de ETL funcionando correctamente"}


@app.post("/upload/")
async def upload_files(files: list[UploadFile] = File(...)):
    process_id = os.urandom(4).hex()
    process_dir = os.path.join(BASE_DIR, process_id)
    os.makedirs(process_dir, exist_ok=True)

    # Inicializar estado
    process_status[process_id] = {"status": "Procesando", "results": []}

    # Leer y guardar todos los archivos antes de iniciar el hilo
    archivos_guardados = []
    for file in files:
        file_path = os.path.join(process_dir, file.filename)
        contents = await file.read()  # Leer todo el contenido en memoria
        with open(file_path, "wb") as f:
            f.write(contents)
        archivos_guardados.append((file.filename, file_path))

    # Función que corre el ETL en segundo plano
    def run_etl():
        for nombre, path in archivos_guardados:
            try:
                datasets = extraer_datasets_from_path(path)
                for ds_nombre, df in datasets.items():
                    try:
                        # Limpieza
                        df_limpio = limpiar_dataset(df)
                        # Detección de outliers
                        outliers = detectar_outliers(df_limpio)

                        # Guardar en Excel y CSV
                        excel_path = os.path.join(process_dir, f"{ds_nombre}_limpio.xlsx")
                        csv_path = os.path.join(process_dir, f"{ds_nombre}_limpio.csv")
                        df_limpio.to_excel(excel_path, index=False)
                        df_limpio.to_csv(csv_path, index=False, encoding="utf-8")

                        # Resumen del dataset
                        resumen = {
                            "filename": ds_nombre,
                            "excel_file": excel_path,
                            "csv_file": csv_path,
                            "rows_original": len(df),
                            "rows_limpio": len(df_limpio),
                            "columns": list(df_limpio.columns),
                            "nulos_por_columna": df_limpio.isnull().sum().to_dict(),
                            "outliers_detectados": len(outliers)
                        }
                        process_status[process_id]["results"].append(resumen)

                    except Exception as e_inner:
                        process_status[process_id]["results"].append({
                            "filename": ds_nombre,
                            "error": str(e_inner)
                        })

            except Exception as e_outer:
                process_status[process_id]["results"].append({
                    "filename": nombre,
                    "error": str(e_outer)
                })

        process_status[process_id]["status"] = "Completado"

    # Ejecutar ETL en segundo plano
    threading.Thread(target=run_etl).start()

    return {"process_id": process_id, "status": "ETL iniciado"}


@app.get("/status/{process_id}")
async def get_status(process_id: str):
    if process_id not in process_status:
        return JSONResponse({"error": "ID de proceso no encontrado"}, status_code=404)
    return process_status[process_id]
