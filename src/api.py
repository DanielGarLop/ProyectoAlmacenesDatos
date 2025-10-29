# src/api.py
import os
import shutil
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from .etl_extraccion import extraer_datasets_from_path
from .etl_transformacion import limpiar_dataset, detectar_outliers
from .etl_carga import cargar_a_excel

UPLOAD_DIR = "uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="API de ETL de Accidentes de Tráfico")

@app.get("/")
async def root():
    return {"message": "API de ETL funcionando correctamente"}

@app.post("/upload/")
async def upload_files(files: list[UploadFile] = File(...)):
    process_id = os.urandom(4).hex()  # Generar ID único para la ejecución
    process_dir = os.path.join(UPLOAD_DIR, process_id)
    os.makedirs(process_dir, exist_ok=True)

    results = []

    for file in files:
        file_path = os.path.join(process_dir, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 🔹 Extracción
        df = extraer_datasets_from_path(file_path)

        # 🔹 Limpieza
        df_limpio = limpiar_dataset(df)

        # 🔹 Detección de outliers
        outliers = detectar_outliers(df_limpio)

        # 🔹 Guardar dataset limpio en Excel
        save_path = os.path.join(process_dir, f"limpio_{file.filename}")
        cargar_a_excel(df_limpio, save_path)

        results.append({
            "filename": file.filename,
            "clean_file": save_path,
            "rows_original": len(df),
            "rows_limpio": len(df_limpio),
            "outliers_detectados": len(outliers)
        })

    return JSONResponse({
        "process_id": process_id,
        "status": "ETL completado",
        "results": results
    })
