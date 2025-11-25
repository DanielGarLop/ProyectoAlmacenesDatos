# src/graficos.py
import matplotlib.pyplot as plt
import os

def graficar_comparacion(resultados, process_dir):
    modelos = list(resultados.keys())
    maes = [resultados[m]['mae'] for m in modelos]
    r2s = [resultados[m]['r2'] for m in modelos]

    plt.figure(figsize=(8,4))
    plt.bar(modelos, maes, color='skyblue')
    plt.title("Comparación MAE")
    plt.ylabel("MAE")
    plt.savefig(os.path.join(process_dir, "mae_comparacion.png"))
    plt.close()

    plt.figure(figsize=(8,4))
    plt.bar(modelos, r2s, color='salmon')
    plt.title("Comparación R²")
    plt.ylabel("R²")
    plt.savefig(os.path.join(process_dir, "r2_comparacion.png"))
    plt.close()
