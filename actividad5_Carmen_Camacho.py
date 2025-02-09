# UNIVERSITAT CARLEMANY
# Actividad 1: Paralelización, test y profiling de aplicaciones
# Docente Ramon Amela Milian
# Fecha de entrega 10-02-2025
# Carmen De Los Ángeles Camacho Tejada


# EJERCICIO 5:
# Separar el programa del punto 4 en las diferentes funciones que se desean paralelizar.

import requests
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Se definen los 10 símbolos
symbols = ["AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META", "NVDA", "SPY", "V"]


# Se obtienen los datos históricos de un símbolo desde Yahoo Finance.
def obtener_datos_simbolo(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    start_date = datetime(2008, 2, 1)
    end_date = datetime(2024, 12, 31)
    period1 = int(start_date.timestamp())
    period2 = int(end_date.timestamp())
    params = {
        "formatted": "true",
        "interval": "1d",
        "includeAdjustedClose": "false",
        "period1": period1,
        "period2": period2,
        "symbol": symbol,
    }
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        response_json = response.json()
        if 'chart' in response_json and 'result' in response_json['chart']:
            result = response_json["chart"]["result"][0]
            timestamps = result["timestamp"]
            formatted_timestamps = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d') for x in timestamps]
            # Se obtienen los datos de cada indicador
            data = pd.DataFrame({
                "date": formatted_timestamps,
                f"{symbol}_high": result["indicators"]["quote"][0]["high"],
                f"{symbol}_low": result["indicators"]["quote"][0]["low"],
                f"{symbol}_open": result["indicators"]["quote"][0]["open"],
                f"{symbol}_close": result["indicators"]["quote"][0]["close"],
                f"{symbol}_volume": result["indicators"]["quote"][0]["volume"],
            })
            # Se convierte la columna de fecha a datetime
            data["date"] = pd.to_datetime(data["date"])

            # Se guarda CSV
            data.to_csv(f"{symbol}_data.csv", index=False)
            print(f"Datos descargados para {symbol}")
            return data
        else:
            print(f"No se encontraron datos para {symbol}")
    else:
        print(f"Error al obtener datos de {symbol}: {response.status_code}")
    return None


# Se descarga y combina los datos de todos los símbolos en paralelo
def procesar_datos(symbols):
    df_principal = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = list(executor.map(obtener_datos_simbolo, symbols))
    # Se filtran los valores None
    resultados = [df for df in resultados if df is not None]
    if resultados:
        df_principal = pd.concat(resultados, axis=1)
        # Se extraen solo una versión de la columna "date" y se asegura de que sea 1D
        if "date" in df_principal.columns:
            fecha = df_principal["date"].iloc[:, 0] if isinstance(df_principal["date"], pd.DataFrame) else df_principal["date"]
            df_principal.drop(columns=["date"], inplace=True)
            df_principal.insert(0, "date", fecha)
        df_principal.set_index("date", inplace=True)

    return df_principal


# Se calculan los datos agregados por semana
def agregar_datos_semanales(df):
    df_semanal = df.resample('W').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    })
    # Se agregan las columnas combinadas de mínimo, máximo y volumen total
    df_semanal['minimo'] = df_semanal.filter(like='_low').min(axis=1)
    df_semanal['maximo'] = df_semanal.filter(like='_high').max(axis=1)
    df_semanal['volumen'] = df_semanal.filter(like='_volume').sum(axis=1)
    # Se mantienen solo las columnas esenciales
    df_semanal = df_semanal[['minimo', 'maximo', 'volumen']]
    df_semanal['year_week'] = df_semanal.index.to_period('W')

    return df_semanal


# Se calculan los datos agregados por mes
def agregar_datos_mensuales(df):
    df_mensual = df.resample('ME').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    })
    # Se agregan columnas combinadas de mínimo, máximo y volumen total
    df_mensual['minimo'] = df_mensual.filter(like='_low').min(axis=1)
    df_mensual['maximo'] = df_mensual.filter(like='_high').max(axis=1)
    df_mensual['volumen'] = df_mensual.filter(like='_volume').sum(axis=1)
    # Se mantienen solo las columnas esenciales
    df_mensual = df_mensual[['minimo', 'maximo', 'volumen']]
    df_mensual['month'] = df_mensual.index.to_period('M')

    return df_mensual


# Se combinan datos semanales y mensuales
def combinar_datos(df_semanal, df_mensual):
    # Se asegura que los índices sean DatetimeIndex
    if isinstance(df_semanal.index, pd.PeriodIndex):
        df_semanal.index = df_semanal.index.to_timestamp()
    if isinstance(df_mensual.index, pd.PeriodIndex):
        df_mensual.index = df_mensual.index.to_timestamp()

    # Se unen los DataFrames
    df_combinado = pd.concat([df_semanal, df_mensual])

    # Se ordenan por fecha
    df_combinado = df_combinado.sort_index()

    return df_combinado


# Función principal que lo ejecuta
def main():
    print("Descargando datos en paralelo...")
    df_principal = procesar_datos(symbols)

    if df_principal.empty:
        print("No se obtuvieron datos. Verificar conexión o API.")
        return

    print("\nDatos combinados:")
    print(df_principal.head())

    # Se generan datos semanales
    df_semanal = agregar_datos_semanales(df_principal)
    print("\nDatos agregados por semana:")
    print(df_semanal.head())

    # Se generan datos mensuales
    df_mensual = agregar_datos_mensuales(df_principal)
    print("\nDatos agregados por mes:")
    print(df_mensual.head())

    # Se combinan los datos
    df_combinado = combinar_datos(df_semanal, df_mensual)
    print("\nDatos combinados (semanal y mensual):")
    print(df_combinado.head())

    # Se guardan los resultados
    df_principal.to_csv("datos_diarios.csv")
    df_semanal.to_csv("datos_semanales.csv")
    df_mensual.to_csv("datos_mensuales.csv")
    df_combinado.to_csv("datos_combinados.csv")

    print("\nProcesamiento completado y datos guardados.")


if __name__ == "__main__":
    main()
