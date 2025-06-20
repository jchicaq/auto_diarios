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
import funciones_partner as fpartner


# Ruta del archivo CSV de links
archivo_link = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data/links_resumen_diario.csv")

# Ruta del archivo CSV de partner
archivo_partner = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data/reporte_partner_afiliados.csv")

# Ruta del archivo de la base de datos SQLite
ruta_db = Path("C:/Users/PRUEBA/Documents/GitHub/Digital/aut_diarios/data_base/datos_diario_digital.db")



# Nombres de las tablas en la base de datos SQLite
tabla_links = "links_diario"
tabla_partner = "partner_diario"



# Se obtiene el DataFrame inicial a partir del archivo CSV de links
df_inicial_link = flink.insertar_archivo_csv(archivo_link)

# Se obtiene el DataFrame inicial a partir del archivo CSV de partner
df_inicial_partner = flink.insertar_archivo_csv(archivo_partner)



# Variables para almacenar los nombres de las columnas link (c: columnas, lin: link)
clin_periodo             = df_inicial_link.columns[0]
clin_partner             = df_inicial_link.columns[1]
clin_pais                = df_inicial_link.columns[2]
clin_id_link             = df_inicial_link.columns[3]
clin_nombre_link         = df_inicial_link.columns[4]
clin_id_afiliador        = df_inicial_link.columns[5]
clin_nombre_afiliador    = df_inicial_link.columns[6]
clin_registros           = df_inicial_link.columns[7]
clin_ftd                 = df_inicial_link.columns[8]
clin_valor_ftd           = df_inicial_link.columns[9]
clin_ftd_puntos_venta    = df_inicial_link.columns[10]
clin_ftd_pasarela        = df_inicial_link.columns[11]

# Variables para almacenar los nombres de las columnas partner (co: columnas, par: partner)
cpar_periodo            = df_inicial_partner.columns[0]
cpar_partner            = df_inicial_partner.columns[1]
cpar_pais               = df_inicial_partner.columns[2]
cpar_usuarios_nuevos    = df_inicial_partner.columns[3]
cpar_ftd                = df_inicial_partner.columns[4]
cpar_usuarios_deposito  = df_inicial_partner.columns[5]
cpar_activo_deportivas  = df_inicial_partner.columns[6]
cpar_activo_casino      = df_inicial_partner.columns[7]
cpar_ftd_pasarela       = df_inicial_partner.columns[8]


# Se organizan las columnas del DataFrame inicial de link
df_inicial_link = flink.convertir_fechas(df_inicial_link, clin_periodo)

# se organizan las columnas del DataFrame inicial de partner
df_inicial_partner = flink.convertir_fechas(df_inicial_partner, cpar_periodo)



# Se convierten las variables de la columna partnern y pais de links
df_link = flink.convertir_variables(df_inicial_link, clin_partner, clin_pais)

# Se convierten las variables de la columna partnern y pais de partners
df_partner = fpartner.convertir_partner_paises(df_inicial_partner, cpar_partner, cpar_pais)



# Se convierten las columnas de tipo numérico a float
df_link = flink.columnas_numericas(df_link, clin_registros, clin_ftd, clin_valor_ftd, clin_ftd_puntos_venta, clin_ftd_pasarela)

# Se convierten las columnas de tipo numérico a int
df_partner = fpartner.convertir_formato_numerico(df_partner, cpar_usuarios_nuevos, cpar_ftd, cpar_usuarios_deposito, cpar_activo_deportivas, cpar_activo_casino, cpar_ftd_pasarela)



# Guardar el archivo CSV de links procesado en un archivo SQLite
fsql.guardar_en_sqlite(df_link, tabla_links, ruta_db, if_exists="replace")

# Guardar el archivo CSV de partner procesado en un archivo SQLite
fsql.guardar_en_sqlite(df_partner, tabla_partner, ruta_db, if_exists="replace")







"""
# print("\nDataFrame inicial de partner:\n")
# print(df_inicial_partner)


# print(f"\n🔍 Valores únicos en la columna {cpar_partner}\n:")
# print(df_inicial_partner[cpar_partner].unique())
# print(f"Total de valores únicos: {df_inicial_partner[cpar_partner].nunique()}")
# print(f"\nValores de {cpar_periodo} donde {cpar_partner} == 'vsft.tech':")
# print("\n Busqueda de VSFT\n")
# print(df_inicial_partner.loc[df_inicial_partner[cpar_partner] == "VSFT", cpar_pais].unique())
# print("\n Busqueda de vsft.tech\n")
# print(df_inicial_partner.loc[df_inicial_partner[cpar_partner] == "vsft.tech", cpar_pais].unique())
# print("\n Busqueda de Virtualsoft\n")
# print(df_inicial_partner.loc[df_inicial_partner[cpar_partner] == "Virtualsoft", cpar_pais].unique())

# print(f"\n🔍 Valores únicos en la columna {cpar_pais}:")
# print(df_inicial_partner[cpar_pais].unique())
# print(f"Total de valores únicos: {df_inicial_partner[cpar_pais].nunique()}")
"""