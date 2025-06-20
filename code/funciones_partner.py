# ----------------------------------------------------------------------
# Autor: Jacobo Chica
#
# Fecha: 20/06/2025
#
# Descripción: Script inicial para la automatización de los informes diarios
# en donde se leen los archivos CSV y se preparan para su procesamiento.
# ----------------------------------------------------------------------

# Importar las librerías necesarias
import pandas as pd
from pathlib import Path


def convertir_partner_paises(df_inicial: pd.DataFrame, partner: str, pais: str) -> pd.DataFrame:
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
        'Paniplay.com': 'Paniplay',
        'GANGABET.COM': 'Gangabet',
        'doradobet': 'Doradobet',
        'vsft.tech': 'VSFT',
        'Virtualsoft': 'VSFT'
    })

    # Reemplazos en columna de país
    df[pais] = df[pais].replace({
        'Per�': 'Perú',
        'M�xico': 'México',
        'Panam�': 'Panamá',
        'Venezuela 2': 'Venezuela',
    })

    # Verificación de que los reemplazos se realizaron correctamente
    valores_erroneos_partner = df[partner].isin(['ecuabet.com', 'Paniplay.com', 'GANGABET.COM', 'doradobet', 'vsft.tech', 'Virtualsoft']).any()
    valores_erroneos_pais = df[pais].isin(['Per�', 'M�xico', 'Panam�', 'Venezuela 2']).any()

    if valores_erroneos_partner or valores_erroneos_pais:
        print("\n ⚠️  Algunos cambios quedaron incompletos.\n")
        if valores_erroneos_partner:
            print("\n   - Todavía hay valores incorrectos en la columna de partner.\n")
        if valores_erroneos_pais:
            print("\n   - Todavía hay valores incorrectos en la columna de país.\n")
    else:
        print("\n ✅ Cambios de normalización aplicados exitosamente.\n")

    return df



def convertir_formato_numerico(df_inicial: pd.DataFrame,
                                  usuario_nuevos: str,
                                  ftd: str,
                                  depositos: str,
                                  act_deportiva: str,
                                  act_casino: str,
                                  ftd_pasarela: str) -> pd.DataFrame:
    """
    Formatea columnas numéricas del DataFrame y reporta la cantidad de valores vacíos en cada una.

    Parámetros:
    ----------
    df : pd.DataFrame
        DataFrame con los datos originales.

    usuario_nuevos : str
        Nombre de la columna con usuarios nuevos (debe convertirse a entero).

    ftd : str
        Nombre de la columna con Primera Recarga (debe convertirse a entero).

    depositos : str
        Nombre de la columna con el valor de los usuarios que depositaron (debe convertirse a entero).

    act_deportiva : str
        Nombre de la columna de activos deportivas (debe convertirse a entero).

    act_casino : str
        Nombre de la columna de activos de casino (debe convertirse a entero).

    ftd_pasarela : str
        Nombre de la columna de activos de primer deposito por pasarela (debe convertirse a entero).

    Retorna:
    -------
    pd.DataFrame
        El DataFrame con las columnas numéricas convertidas y reporte de vacíos.
    """

    # Copia de trabajo para evitar modificar el original
    df = df_inicial.copy()

    columnas_int = [usuario_nuevos, ftd, depositos, act_deportiva, act_casino, ftd_pasarela]


    # Reemplazar comas por puntos en columnas numéricas, si es necesario
    for col in columnas_int:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False).str.strip()

    # Convertir a int
    for col in columnas_int:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Contar valores nulos en cada columna transformada y reportar si hay alguno
    for col in columnas_int:
        num_nulos = df[col].isna().sum()
        if num_nulos > 0:
            print(f"La columna '{col}' de partners_diarios tiene {num_nulos} valores nulos.")

    return df












