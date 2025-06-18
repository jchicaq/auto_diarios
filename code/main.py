# ----------------------------------------------------------------------
# Autor: Jacobo Chica
#
# Fecha: 18/06/2025
#
# Descripción: Script inicial para la automatización de los informes diarios
# en donde se leen los archivos CSV y se preparan para su procesamiento.
# ----------------------------------------------------------------------

# Importar las librerías necesarias
import pandas as pd
import re
from pathlib import Path


# Definir la ruta del archivo CSV y verificar su existencia
try:
    archivo = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data/links_resumen_diario.csv")
    if not archivo.exists():
        print("No se encontró el archivo.")
        exit(1)
except Exception as e:
    print("Ocurrió un error al intentar acceder al archivo:", e)
    exit(1)


# Leer el CSV con separador ';', permitir comillas dobles
df_inicial = pd.read_csv(
    archivo,
    sep=';',
    quotechar='"',
    dtype=str,  
    encoding='utf-8',
    skip_blank_lines=True,
    engine='python'
    )


# Copia del dataframe para probar formato con dayfirst=True y encontrar la fecha de corte
df_inicial_copia = df_inicial.copy()
df_inicial_copia['Periodo'] = pd.to_datetime(df_inicial_copia['Periodo'], dayfirst=True, errors='coerce')


# Encontrar la primera fila con fecha no reconocida (NaT)
fecha_corte = df_inicial_copia['Periodo'].isna().idxmax()
print(f"\n 🔍 Cambio de formato detectado en el índice: {fecha_corte}\n")


# Separar el DataFrame en dos partes para manejar diferentes formatos de fecha 
df_1 = df_inicial.iloc[:fecha_corte].copy()   # Parte con fechas en formato dd/mm/yyyy
df_2 = df_inicial.iloc[fecha_corte:].copy()   # Parte con fechas en formato yyyy-mm-dd


print("Número de filas en df_1:", len(df_1))
print("Número de filas en df_2:", len(df_2))

df_1['Periodo'] = pd.to_datetime(df_1['Periodo'], dayfirst=True, errors='coerce')
df_2['Periodo'] = pd.to_datetime(df_2['Periodo'], format='%Y-%m-%d', errors='coerce')

# Uniformar el formato a dd/mm/yyyy (esto convierte a string)
df_1['Periodo'] = df_1['Periodo'].dt.strftime('%d/%m/%Y')
df_2['Periodo'] = df_2['Periodo'].dt.strftime('%d/%m/%Y')


# print(df_1.tail(5))  # Mostrar las últimas 5 filas de df_1
# print(df_2.head(5))  # Mostrar las primeras 5 filas de df_2


# Unir los dos DataFrames de nuevo
df = pd.concat([df_1, df_2], ignore_index=True)

print(df.iloc[fecha_corte-2:].head(5))  # Mostrar 5 filas a partir de la fila "fecha_corte" del DataFrame final


# # Encontrar el índice de la primera fila con dato nulo en la columna 'Periodo'
# primer_null = df['Periodo'].isna() | (df['Periodo'] == '')
# if primer_null.any():
#     print(f"Número de la primera fila con dato Null en 'Periodo': {primer_null.idxmax()}")
# else:
#     print("No hay filas con datos Null en la columna 'Periodo'.")


# # Contar cuántas filas vacías (NaN o cadenas vacías) tiene la columna "Periodo"
# num_filas_vacias = df['Periodo'].isna().sum() + (df['Periodo'] == '').sum()
# print(f"Número de filas vacías en la columna 'Periodo': {num_filas_vacias}")







