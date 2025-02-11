from fastapi import FastAPI, UploadFile  # FastAPI para la API
import pandas as pd  # Pandas para leer el Excel
import sqlite3  # SQLite para la base de datos
import json  # Para convertir datos en formato JSON
import os

app = FastAPI()  # Crear la instancia de FastAPI

# Endpoint para subir y procesar el Excel
@app.post("/importar-excel/")
async def importar_excel(file: UploadFile):  # Recibe el archivo como parámetro
    # Leer el archivo Excel usando Pandas
    df = pd.read_excel(file.file)  # Lee el contenido del Excel y lo convierte en DataFrame

    # Conexión a la base de datos SQLite
    # Obtener la ruta absoluta al directorio actual (donde está el archivo app.py)
    base_path = os.path.dirname(os.path.abspath(__file__))

    # Crear la ruta completa a la base de datos usando la ruta relativa
    db_path = os.path.join(base_path, "../db/mi_base.db")

    # Conectar a la base de datos
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Crear la tabla si no existe, basada en las columnas del Excel
    columnas = ", ".join([f'"{col}" TEXT' for col in df.columns])  # Crear columnas dinámicas según el Excel
    sql_create = f'CREATE TABLE IF NOT EXISTS datos_importados (id INTEGER PRIMARY KEY AUTOINCREMENT, {columnas}, datos_extra JSON)'
    cursor.execute(sql_create)  # Ejecutar la creación de la tabla

    # Insertar los datos del Excel en la base de datos
    for _, row in df.iterrows():  # Iterar sobre cada fila del DataFrame
        # Crear un campo JSON para las columnas que no están previstas
        datos_extra = {col: row[col] for col in df.columns if col not in ['nombre', 'fecha']}  # Excluir columnas fijas
        sql_insert = f'INSERT INTO datos_importados (nombre, fecha, datos_extra) VALUES (?, ?, ?)'  # Consulta de inserción
        cursor.execute(sql_insert, (row['nombre'], row['fecha'], json.dumps(datos_extra)))  # Insertar en la base de datos

    conn.commit()  # Guardar los cambios en la base de datos
    conn.close()  # Cerrar la conexión

    return {"mensaje": "Datos importados exitosamente"}  # Respuesta exitosa
