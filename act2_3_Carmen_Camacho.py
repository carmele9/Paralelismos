# UNIVERSITAT CARLEMANY
# Actividad 2: Utilización de la librería asyncio
# Docente Ramon Amela Milian
# Fecha de entrega 17-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 3:
# Modificar el programa de análisis de mercados para ejecutar todas las funciones
# paralelas sean ejecutadas en una corutina.
# Realizar una versión de los algoritmos 2 utilizando la librería requests

# requests con create_task() sin to_thread()
# (requests bloqueante, pero lanzando cada descarga con asyncio.create_task())

import asyncio
import requests
import pandas as pd
from datetime import datetime
import sys

symbols = ["AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META", "NVDA", "SPY", "V"]


# Se realiza una función bloqueante para obtener datos de un símbolo
def obtener_datos_simbolo_sync(symbol):
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
        "user-agent": "Mozilla/5.0"
    }

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


# Se realiza una versión asíncrona pero bloqueante usando create_task() y la función sincronizada
async def obtener_datos_simbolo(symbol):
    return obtener_datos_simbolo_sync(symbol)


# Se realiza una función para procesar todos los símbolos
async def procesar_datos(symbols):
    # Se usa el create_task() para crear las tareas
    tareas = [asyncio.create_task(obtener_datos_simbolo(symbol)) for symbol in symbols]
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
    else:
        return pd.DataFrame()



# Agregación de datos semanales
async def agregar_datos_semanales(df):
    return df.resample('W').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    })


# Agregación de datos mensuales
async def agregar_datos_mensuales(df):
    return df.resample('ME').agg({
        **{col: 'max' for col in df.columns if '_high' in col},
        **{col: 'min' for col in df.columns if '_low' in col},
        **{col: 'first' for col in df.columns if '_open' in col},
        **{col: 'last' for col in df.columns if '_close' in col},
        **{col: 'sum' for col in df.columns if '_volume' in col},
    })


# Combinación de datos semanales y mensuales
async def combinar_datos(df_semanal, df_mensual):
    if isinstance(df_semanal.index, pd.PeriodIndex):
        df_semanal.index = df_semanal.index.to_timestamp()
    if isinstance(df_mensual.index, pd.PeriodIndex):
        df_mensual.index = df_mensual.index.to_timestamp()

    df_combinado = pd.concat([df_semanal, df_mensual])
    df_combinado = df_combinado.sort_index()
    return df_combinado


# Ejecución principal
async def main():
    try:
        print("Descargando datos usando 'requests' con 'create_task()' (sin 'to_thread')...")
        df_principal = await procesar_datos(symbols)

        if df_principal.empty:
            print("No se obtuvieron datos. Verificar conexión o API.")
            return

        print("\nDatos combinados (diarios):")
        print(df_principal.head())

        # Agregaciones semanales y mensuales en paralelo
        tarea_semanal = asyncio.create_task(agregar_datos_semanales(df_principal))
        tarea_mensual = asyncio.create_task(agregar_datos_mensuales(df_principal))
        df_semanal, df_mensual = await asyncio.gather(tarea_semanal, tarea_mensual)

        print("\nDatos agregados por semana:")
        print(df_semanal.head())

        print("\nDatos agregados por mes:")
        print(df_mensual.head())

        # Combinamos datos
        df_combinado = await combinar_datos(df_semanal, df_mensual)
        print("\nDatos combinados (semanal y mensual):")
        print(df_combinado.head())

        # Guardamos los resultados
        df_principal.to_csv("datos_diarios_requests_sin_to_thread.csv")
        df_semanal.to_csv("datos_semanales_requests_sin_to_thread.csv")
        df_mensual.to_csv("datos_mensuales_requests_sin_to_thread.csv")
        df_combinado.to_csv("datos_combinados_requests_sin_to_thread.csv")

        print("\nProcesamiento completado y datos guardados.")
    except Exception as e:
        print(f"Error en la ejecución: {e}")

# Se ejecuta el programa
if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # Debido a un error con mi entorno y Windows
    asyncio.run(main())
