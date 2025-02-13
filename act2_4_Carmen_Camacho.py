# UNIVERSITAT CARLEMANY
# Actividad 2: Utilización de la librería asyncio
# Docente Ramon Amela Milian
# Fecha de entrega 17-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 4:
# Modificar el programa de análisis de mercados para ejecutar todas las funciones
# paralelas sean ejecutadas en una corutina.
# Realizar una versión de los algoritmos 2 utilizando la librería aoihttp

# aiohttp con create_task() y to_thread()
# (se fuerza el uso de to_thread() envolviendo la solicitud de aiohttp)

import aiohttp
import asyncio
import pandas as pd
import sys
from datetime import datetime

symbols = ["AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META", "NVDA", "SPY", "V"]


# Se usa una función bloqueante para realizar la solicitud con aiohttp dentro de to_thread
def fetch_data_sync(symbol):
    async def fetch():
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

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    response_json = await response.json()
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
                print(f"Error al obtener datos de {symbol}: {response.status}")
        return None

    return asyncio.run(fetch())


# Se usa una función asíncrona para obtener los datos usando to_thread()
async def obtener_datos_simbolo(symbol):
    return await asyncio.to_thread(fetch_data_sync, symbol)


# Se realiza un a función asíncrona para procesar los datos
async def procesar_datos(symbols):
    df_principal = pd.DataFrame()
    # Se crea las tareas con el create_task()
    tareas = [asyncio.create_task(obtener_datos_simbolo(symbol)) for symbol in symbols]
    resultados = await asyncio.gather(*tareas)

    resultados = [df for df in resultados if df is not None]
    if resultados:
        df_principal = pd.concat(resultados, axis=1)
        if "date" in df_principal.columns:
            fecha = df_principal["date"].iloc[:, 0] if isinstance(df_principal["date"], pd.DataFrame) else df_principal[
                "date"]
            df_principal.drop(columns=["date"], inplace=True)
            df_principal.insert(0, "date", fecha)
        df_principal.set_index("date", inplace=True)

    return df_principal


# Se agregan los datos semanales usando una función asíncrona
async def agregar_datos_semanales(df):
    return await asyncio.to_thread(lambda: df.resample('W').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    }))


# Se agregan los datos mensuales usando una función asíncrona
async def agregar_datos_mensuales(df):
    return await asyncio.to_thread(lambda: df.resample('ME').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    }))


# Se combinan los datos usando una función asíncrona
async def combinar_datos(df_semanal, df_mensual):
    if isinstance(df_semanal.index, pd.PeriodIndex):
        df_semanal.index = df_semanal.index.to_timestamp()
    if isinstance(df_mensual.index, pd.PeriodIndex):
        df_mensual.index = df_mensual.index.to_timestamp()

    df_combinado = pd.concat([df_semanal, df_mensual])
    df_combinado = df_combinado.sort_index()
    return df_combinado


# Se realiza el programa principal
async def main():
    print("Descargando datos en paralelo...")
    df_principal = await procesar_datos(symbols)

    if df_principal.empty:
        print("No se obtuvieron datos. Verificar conexión o API.")
        return

    print("\nDatos combinados:")
    print(df_principal.head())

    # Agregaciones semanales y mensuales en paralelo
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

    # Se guardan los resultados
    await asyncio.to_thread(df_principal.to_csv, "datos_diarios.csv")
    await asyncio.to_thread(df_semanal.to_csv, "datos_semanales.csv")
    await asyncio.to_thread(df_mensual.to_csv, "datos_mensuales.csv")
    await asyncio.to_thread(df_combinado.to_csv, "datos_combinados.csv")

    print("\nProcesamiento completado y datos guardados.")


# Se ejecuta el programa
if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
