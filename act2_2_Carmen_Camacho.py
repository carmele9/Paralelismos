# UNIVERSITAT CARLEMANY
# Actividad 2: Utilización de la librería asyncio
# Docente Ramon Amela Milian
# Fecha de entrega 17-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 2:
# Modificar el programa de análisis de mercados para ejecutar todas las funciones
# paralelas sean ejecutadas en una corutina.
# Realizar una versión donde se utilice el mecanismo to_thread()


import asyncio
import requests
from datetime import datetime
import pandas as pd
import sys

# Se definen los 10 símbolos
symbols = ["AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META", "NVDA", "SPY", "V"]


# Se realiza una función asíncrona para obtener los datos históricos del mercado de valores
async def obtener_datos_simbolo(symbol):
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

    def fetch():
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response_json = response.json()
            if 'chart' in response_json and 'result' in response_json['chart']:
                result = response_json["chart"]["result"][0]
                timestamps = result["timestamp"]
                formatted_timestamps = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d') for x in timestamps]
                data = pd.DataFrame({
                    "date": formatted_timestamps,
                    f"{symbol}_high": result["indicators"]["quote"][0]["high"],
                    f"{symbol}_low": result["indicators"]["quote"][0]["low"],
                    f"{symbol}_open": result["indicators"]["quote"][0]["open"],
                    f"{symbol}_close": result["indicators"]["quote"][0]["close"],
                    f"{symbol}_volume": result["indicators"]["quote"][0]["volume"],
                })
                data["date"] = pd.to_datetime(data["date"])
                data.to_csv(f"{symbol}_data.csv", index=False)
                print(f"Datos descargados para {symbol}")
                return data
            else:
                print(f"No se encontraron datos para {symbol}")
        else:
            print(f"Error al obtener datos de {symbol}: {response.status_code}")
        return None

    return await asyncio.to_thread(fetch)


# Se descarga y combinan los datos de todos los símbolos en paralelo
async def procesar_datos(symbols):
    df_principal = pd.DataFrame()
    tareas = [obtener_datos_simbolo(symbol) for symbol in symbols]
    resultados = await asyncio.gather(*tareas)
    resultados = [df for df in resultados if df is not None]

    if resultados:
        df_principal = pd.concat(resultados, axis=1)
        if "date" in df_principal.columns:
            fecha = df_principal["date"].iloc[:, 0] if isinstance(df_principal["date"], pd.DataFrame) else df_principal["date"]
            df_principal.drop(columns=["date"], inplace=True)
            df_principal.insert(0, "date", fecha)
        df_principal.set_index("date", inplace=True)

    return df_principal


# Se realiza una función asíncrona para calcular los datos semanales
async def agregar_datos_semanales(df):
    return await asyncio.to_thread(lambda: df.resample('W').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    }))


# Se realiza una función asíncrona para calcular los datos mensuales
async def agregar_datos_mensuales(df):
    return await asyncio.to_thread(lambda: df.resample('ME').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    }))


# Se combinan los datos semanales y mensuales
async def combinar_datos(df_semanal, df_mensual):
    if isinstance(df_semanal.index, pd.PeriodIndex):
        df_semanal.index = df_semanal.index.to_timestamp()
    if isinstance(df_mensual.index, pd.PeriodIndex):
        df_mensual.index = df_mensual.index.to_timestamp()

    df_combinado = pd.concat([df_semanal, df_mensual])
    df_combinado = df_combinado.sort_index()
    return df_combinado


# Se emplea una función asincrónica para realizar el programa
async def main():
    print("Descargando datos en paralelo...")
    df_principal = await procesar_datos(symbols)

    if df_principal.empty:
        print("No se obtuvieron datos. Verificar conexión o API.")
        return

    print("\nDatos combinados:")
    print(df_principal.head())

    # Se ejecutan las funciones de agregación en paralelo
    tarea_semanal = asyncio.create_task(agregar_datos_semanales(df_principal))
    tarea_mensual = asyncio.create_task(agregar_datos_mensuales(df_principal))
    df_semanal, df_mensual = await asyncio.gather(tarea_semanal, tarea_mensual)

    print("\nDatos agregados por semana:")
    print(df_semanal.head())

    print("\nDatos agregados por mes:")
    print(df_mensual.head())

    df_combinado = await combinar_datos(df_semanal, df_mensual)
    print("\nDatos combinados (semanal y mensual):")
    print(df_combinado.head())

    # Se guardan los resultados usando to_thread()
    await asyncio.to_thread(df_principal.to_csv, "datos_diarios.csv")
    await asyncio.to_thread(df_semanal.to_csv, "datos_semanales.csv")
    await asyncio.to_thread(df_mensual.to_csv, "datos_mensuales.csv")
    await asyncio.to_thread(df_combinado.to_csv, "datos_combinados.csv")

    print("\nProcesamiento completado y datos guardados.")

# Se ejecuta el programa
    if __name__ == "__main__":
        print("HOLA")
        if sys.platform.startswith("win"):  # Debido a un problema con mi entorno y Windows
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
