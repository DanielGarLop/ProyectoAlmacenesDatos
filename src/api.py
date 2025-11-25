# src/api.py
import os
import uuid
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
import threading
from typing import List

from etl_transformacion import limpiar_dataset, detectar_outliers
from etl_carga import cargar_dataframe
from mineria_datos import entrenar_modelos
from graficos import graficar_comparacion

BASE_DIR = "uploaded_files"
os.makedirs(BASE_DIR, exist_ok=True)

process_status = {}

app = FastAPI(title="API de ETL + Minería de Datos")

@app.get("/")
async def root():
    return {"message": "API funcionando correctamente"}

@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...), objetivo: str = "precio"):
    """
    Sube archivos CSV/Excel y ejecuta ETL + minería de datos en segundo plano.
    `objetivo`: nombre de la columna a predecir
    """
    process_id = uuid.uuid4().hex
    process_dir = os.path.join(BASE_DIR, process_id)
    os.makedirs(process_dir, exist_ok=True)

    process_status[process_id] = {"status": "Procesando", "results": []}

    # Guardar archivos subidos
    archivos_guardados = []
    for file in files:
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in [".csv", ".xlsx"]:
            continue
        file_path = os.path.join(process_dir, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())
        archivos_guardados.append(file_path)

    def run_etl():
        for file_path in archivos_guardados:
            nombre = os.path.basename(file_path)
            try:
                # Leer archivo
                if file_path.endswith(".csv"):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)

                # 1️⃣ Limpieza
                df_limpio = limpiar_dataset(df)

                # 2️⃣ Detección de outliers
                outliers_df = detectar_outliers(df_limpio)

                # 3️⃣ Guardar archivos limpios
                excel_path = os.path.join(process_dir, f"{nombre}_limpio.xlsx")
                csv_path = os.path.join(process_dir, f"{nombre}_limpio.csv")
                cargar_dataframe(df_limpio, excel_path, formato="excel")
                cargar_dataframe(df_limpio, csv_path, formato="csv")

                # 4️⃣ Minería de datos: entrenar modelos
                resultados_modelos, scaler, _ = entrenar_modelos(df_limpio, objetivo=objetivo)

                # 5️⃣ Graficar desempeño
                graficar_comparacion(resultados_modelos, process_dir)

                # 6️⃣ Guardar resumen en process_status
                process_status[process_id]["results"].append({
                    "filename": nombre,
                    "rows_original": len(df),
                    "rows_limpio": len(df_limpio),
                    "outliers_detectados": len(outliers_df),
                    "modelos": {m: {"mae": resultados_modelos[m]["mae"], "r2": resultados_modelos[m]["r2"]} 
                                for m in resultados_modelos},
                    "graficos": {
                        "mae": os.path.join(process_dir, "mae_comparacion.png"),
                        "r2": os.path.join(process_dir, "r2_comparacion.png")
                    }
                })

            except Exception as e:
                process_status[process_id]["results"].append({
                    "filename": nombre,
                    "error": str(e)
                })

        process_status[process_id]["status"] = "Completado"

    threading.Thread(target=run_etl).start()

    return {"process_id": process_id, "status": "ETL + Minería de datos iniciado"}

@app.get("/status/{process_id}")
async def get_status(process_id: str):
    if process_id not in process_status:
        return JSONResponse({"error": "ID de proceso no encontrado"}, status_code=404)
    return process_status[process_id]

@app.get("/download/{process_id}/{tipo}")
async def download_graph(process_id: str, tipo: str):
    """
    Descarga gráficos generados por los modelos.
    `tipo` puede ser 'mae' o 'r2'
    """
    if process_id not in process_status:
        return JSONResponse({"error": "ID de proceso no encontrado"}, status_code=404)

    # Tomar la primera entrada de resultados para el archivo
    if not process_status[process_id]["results"]:
        return JSONResponse({"error": "Aún no hay resultados generados"}, status_code=404)

    file_path = process_status[process_id]["results"][0]["graficos"].get(tipo)
    if not file_path or not os.path.exists(file_path):
        return JSONResponse({"error": "Archivo no encontrado"}, status_code=404)

    return FileResponse(file_path, media_type="image/png", filename=os.path.basename(file_path))
