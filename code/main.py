# ----------------------------------------------------------------------
# Autor: Jacobo Chica
#
# Fecha: 19/06/2025
#
# Descripción: Este archivo es el punto de entrada principal para la automatización de los informes diarios.
# Se encarga de leer los archivos de links y resumen necesarios y preparar los datos para su posterior procesamiento.
# Desde este script se ejecutan las funciones definidas en otros módulos del proyecto.
# ----------------------------------------------------------------------

# Importar las librerías necesarias
import pandas as pd
from pathlib import Path
import funciones_link as flink
import funciones_sql as fsql


# Ruta del archivo CSV de links
archivo_link = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data/links_resumen_diario.csv")

# Ruta del archivo de la base de datos SQLite
ruta_db = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data_base/datos_diario_digital.db")

# Nombres de las tablas en la base de datos SQLite
tabla_links = "links_diario"


# Se obtiene el DataFrame inicial a partir del archivo CSV de links
df_inicial_link = flink.insertar_archivo_csv(archivo_link)


# Variables para almacenar los nombres de las columnas
col_periodo             = df_inicial_link.columns[0]
col_partner             = df_inicial_link.columns[1]
col_pais                = df_inicial_link.columns[2]
col_id_link             = df_inicial_link.columns[3]
col_nombre_link         = df_inicial_link.columns[4]
col_id_afiliador        = df_inicial_link.columns[5]
col_nombre_afiliador    = df_inicial_link.columns[6]
col_registros           = df_inicial_link.columns[7]
col_ftd                 = df_inicial_link.columns[8]
col_valor_ftd           = df_inicial_link.columns[9]
col_ftd_puntos_venta    = df_inicial_link.columns[10]
col_ftd_pasarela        = df_inicial_link.columns[11]


# Se organizan las columnas del DataFrame inicial
df_inicial_link = flink.convertir_fechas(df_inicial_link, col_periodo)


# Se convierten las variables de la columna partnern y pais
df_link = flink.convertir_variables(df_inicial_link, col_partner, col_pais)


# Se convierten las columnas de tipo numérico a float
df_link = flink.columnas_numericas(df_link, col_registros, col_ftd, col_valor_ftd, col_ftd_puntos_venta, col_ftd_pasarela)


# Guardar el archivo CSV de links procesado en un archivo SQLite
fsql.guardar_en_sqlite(df_link, tabla_links, ruta_db, if_exists="replace")