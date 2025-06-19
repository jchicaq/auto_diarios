# ----------------------------------------------------------------------
# Autor: Jacobo Chica
#
# Fecha: 18/06/2025
#
# Descripción: Script inicial para la automatización de los informes diarios
# en donde se leen los archivos CSV y se preparan para su procesamiento.
# ----------------------------------------------------------------------

# Importar las librerías necesarias
from duckdb import df
import pandas as pd
from pathlib import Path


def insertar_archivo_csv(archivo: Path) -> pd.DataFrame:
    """
    Lee el archivo CSV y retorna el DataFrame resultante.

    Parámetros:
    ----------
    archivo : Path
        Ruta del archivo CSV a leer.

    Retorna:
    -------
    pd.DataFrame
        El DataFrame resultante convirtiendo todos los tipos de datos a string.
    """

    # Verificar si el archivo existe y manejar posibles errores
    try:
      
        if not archivo.exists():
            print("\n No se encontró el archivo. \n")
            return None
        
    except Exception as e:

        print("\n Ocurrió un error al intentar acceder al archivo:", e)
        return None

    # Leer el archivo CSV y crear un DataFrame
    df_inicial = pd.read_csv(
        archivo,
        sep=';',
        quotechar='"',
        dtype=str,
        encoding='utf-8',
        skip_blank_lines=True,
        engine='python'
    )

    # Verificar si el DataFrame está vacío
    if df_inicial.empty:
        print("\n El DataFrame está vacío. Asegúrate de que el archivo CSV contenga datos. \n")
        return None
    
    # Retornar el DataFrame resultante
    print(f" \n ✅ Archivo CSV leído correctamente: {Path(archivo).name}\n")
    return df_inicial



def convertir_fechas(df_inicial: pd.DataFrame, periodo: str) -> pd.DataFrame:
    """
    Convierte la columna de periodo del DataFrame a formato datetime, manejando diferentes formatos de fecha desde una fecha de corte.

    Parámetros:
    ----------
    df_inicial : pd.DataFrame
        DataFrame con la columna de periodo que contiene fechas en diferentes formatos.

    periodo : str
        Nombre de la columna que contiene las fechas a convertir.

    Retorna:
    -------
    pd.DataFrame
        DataFrame con la columna de periodo convertida a formato datetime.
    """

    # Copia del dataframe para probar formato con dayfirst=True y encontrar la fecha de corte
    df_inicial_copia = df_inicial.copy()

    # Convertir la columna 'Periodo' a tipo datetime, tratando de reconocer el formato dd/mm/yyyy
    df_inicial_copia[periodo] = pd.to_datetime(df_inicial_copia[periodo], dayfirst=True, errors='coerce')

    # Encontrar la primera fila con fecha no reconocida (NaT)
    fecha_corte = df_inicial_copia[periodo].isna().idxmax()
    # print(f"\n 🔍 Cambio de formato detectado en el índice: {fecha_corte}\n") # Mostrar la fecha de corte

    # Separar el DataFrame en dos partes para manejar diferentes formatos de fecha 
    df_1 = df_inicial.iloc[:fecha_corte].copy()   # Parte con fechas en formato dd/mm/yyyy
    df_2 = df_inicial.iloc[fecha_corte:].copy()   # Parte con fechas en formato yyyy-mm-dd

    # Convertir la columna 'Periodo' de ambos DataFrames en formato datetime
    df_1[periodo] = pd.to_datetime(df_1[periodo], dayfirst=True, errors='coerce')
    df_2[periodo] = pd.to_datetime(df_2[periodo], format='%Y-%m-%d', errors='coerce')

    # Uniformar el formato a dd/mm/yyyy (esto convierte a string)
    df_1[periodo] = df_1[periodo].dt.strftime('%d/%m/%Y')
    df_2[periodo] = df_2[periodo].dt.strftime('%d/%m/%Y')
    # print(df_1.tail(5))  # Mostrar las últimas 5 filas de df_1
    # print(df_2.head(5))  # Mostrar las primeras 5 filas de df_2

    # Unir los dos DataFrames de nuevo
    df = pd.concat([df_1, df_2], ignore_index=True)

    # Convertir la columna 'Periodo' a tipo datetime
    df[periodo] = pd.to_datetime(df[periodo], dayfirst=True, errors='coerce').dt.date
    # print(df.iloc[fecha_corte-2:].head(5))  # Mostrar 5 filas para ver el cambio de la fila "fecha_corte" del DataFrame final

    # Contar cuántas filas vacías (NaN o cadenas vacías) tiene la columna "Periodo"
    num_filas_vacias = df[periodo].isna().sum() + (df[periodo] == '').sum()
    if num_filas_vacias > 0:
        print(f"Número de filas vacías en la columna {periodo}: {num_filas_vacias}")
    else:
        print(f"✅ Las fechas se transformaron con éxito en la columna {periodo}.")

    return df



def convertir_variables(df_inicial: pd.DataFrame, partner: str, pais: str) -> pd.DataFrame:
    """
    Normaliza valores inconsistentes en las columnas de partner y país del DataFrame.

    Parámetros:
    ----------
    df_inicial : pd.DataFrame
        El DataFrame original con datos a limpiar.

    partner : str
        Nombre de la columna que contiene los valores del partner.
        
    pais : str
        Nombre de la columna que contiene los valores del país.

    Retorna:
    -------
    pd.DataFrame
        El DataFrame resultante con los valores normalizados.
    """

    df = df_inicial.copy()

    # Reemplazos en columna de partner
    df[partner] = df[partner].replace({
        'ecuabet.com': 'Ecuabet',
        'Paniplay.com': 'Paniplay'
    })

    # Reemplazos en columna de país
    df[pais] = df[pais].replace({
        'Per�': 'Perú'
    })

    # Verificación de que los reemplazos se realizaron correctamente
    valores_erroneos_partner = df[partner].isin(['ecuabet.com', 'Paniplay.com']).any()
    valores_erroneos_pais = df[pais].isin(['Per�']).any()

    if valores_erroneos_partner or valores_erroneos_pais:
        print("\n ⚠️  Algunos cambios quedaron incompletos.\n")
        if valores_erroneos_partner:
            print("\n   - Todavía hay valores incorrectos en la columna de partner.\n")
        if valores_erroneos_pais:
            print("\n   - Todavía hay valores incorrectos en la columna de país.\n")
    else:
        print("\n ✅ Cambios de normalización aplicados exitosamente.\n")

    return df



def columnas_numericas(df_inicial: pd.DataFrame,
                                  registros: str,
                                  ftd: str,
                                  valor_ftd: str,
                                  ftd_puntos_venta: str,
                                  ftd_pasarela: str) -> pd.DataFrame:
    """
    Formatea columnas numéricas del DataFrame y reporta la cantidad de valores vacíos en cada una.

    Parámetros:
    ----------
    df : pd.DataFrame
        DataFrame con los datos originales.

    registros : str
        Nombre de la columna con registros (debe convertirse a entero).
    
    ftd : str
        Nombre de la columna con FTD (debe convertirse a entero).

    valor_ftd : str
        Nombre de la columna con el valor del FTD (debe convertirse a float).

    ftd_puntos_venta : str
        Nombre de la columna FTD por punto de venta (debe convertirse a entero).

    ftd_pasarela : str
        Nombre de la columna FTD por pasarela (debe convertirse a entero).

    Retorna:
    -------
    pd.DataFrame
        El DataFrame con las columnas numéricas convertidas y reporte de vacíos.
    """

    # Copia de trabajo para evitar modificar el original
    df = df_inicial.copy()

    columnas_int = [registros, ftd, ftd_puntos_venta, ftd_pasarela]
    columnas_float = [valor_ftd]

    # Reemplazar comas por puntos en columnas numéricas, si es necesario
    for col in columnas_float + columnas_int:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False).str.strip()

    # Convertir a int
    for col in columnas_int:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Convertir a float
    for col in columnas_float:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    return df









