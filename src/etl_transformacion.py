# etl_transformacion.py
import pandas as pd
import numpy as np

def limpiar_dataset(df):
    """Limpieza general de un DataFrame"""
    df = df.drop_duplicates().copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df = df.replace(['NA', 'N/A', 'null', 'na', 'NaN', 'None'], np.nan)

    # Manejo de nulos
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna("Desconocido")

    # Fechas
    for col in df.columns:
        if 'fecha' in col or 'date' in col:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            except:
                pass

    return df

def detectar_outliers(df):
    """Detecta outliers usando IQR y devuelve un reporte"""
    outliers = {}
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers[col] = df[(df[col] < Q1 - 1.5*IQR) | (df[col] > Q3 + 1.5*IQR)].shape[0]
    return outliers
