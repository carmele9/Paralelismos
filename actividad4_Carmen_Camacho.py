# UNIVERSITAT CARLEMANY
# Actividad 1: Paralelización, test y profiling de aplicaciones
# Docente Ramon Amela Milian
# Fecha de entrega 10-02-2025
# Carmen De Los Ángeles Camacho Tejada

#EJERCICIO 4:
# Escoger 10 símbolos en finance.yahoo.com. Realizar un programa que:
# a. Iterar los 10 símbolos y descargue la información diaria entre el 1 de febrero de
# 2008 y el 31 de diciembre de 2024.
# Este paso puede realizarse mediante una petición HTTP. En el campus hay un fichero donde
# realiza esta acción para la acción Amazon (ticker AMZN).
# b. Poner la información en dataframes de pandas
# c. Agregar la información por semanas, de forma que los índices del dataframe
# resultante sean año/semana y contenga el mínimo de la semana, el máximo, la
# apertura, la clausura y la suma de volúmenes.
# d. Agregar la información de forma similar a la anterior pero esta vez hacerlo por meses.
# Finalmente, se quiere agregar la información de forma que haya una fila por semana en el primer
# caso y una fila por mes en el segundo. Habrá 3 columnas, mínimo y máximo (con el mínimo y
# máximo agregado de todas las empresas para esa semana/mes) y una columna volumen con la
# suma de todos los volúmenes.

import requests
from datetime import datetime
import pandas as pd

# Se definen los 10 símbolos
symbols = ["AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "NFLX", "META", "NVDA", "SPY", "V"]


# Se realiza una función para obtener los datos de cada símbolo
def obtener_datos_simbolo(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    # Se convierten las fechas a formato de timestamp UNIX
    start_date = datetime(2008, 2, 1)  # 1 de febrero de 2008
    end_date = datetime(2024, 12, 31)  # 31 de diciembre de 2024
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
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 "
                      "Safari/537.36"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        response_json = response.json()
        if 'chart' in response_json and 'result' in response_json['chart']:
            result = response_json["chart"]["result"][0]
            timestamps = result["timestamp"]
            formatted_timestamps = list(map(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'), timestamps))

            # Se obtiene los datos de cada indicador
            high = result["indicators"]["quote"][0]["high"]
            low = result["indicators"]["quote"][0]["low"]
            open_price = result["indicators"]["quote"][0]["open"]
            close = result["indicators"]["quote"][0]["close"]
            volume = result["indicators"]["quote"][0]["volume"]

            # Se crea un DataFrame con los resultados
            data = pd.DataFrame({
                "date": formatted_timestamps,
                "high": high,
                "low": low,
                "open": open_price,
                "close": close,
                "volume": volume,
            })

            # Se convierte la columna 'date' a tipo datetime para permitir la reagrupación
            data["date"] = pd.to_datetime(data["date"])

            # Se renombran las columnas para evitar duplicados
            data = data.rename(columns={
                "high": f"{symbol}_high",
                "low": f"{symbol}_low",
                "open": f"{symbol}_open",
                "close": f"{symbol}_close",
                "volume": f"{symbol}_volume"
            })

            # Se guardan los datos en un archivo CSV
            data.to_csv(f"{symbol}_data.csv", index=False)
            print(f"Datos descargados para el símbolo {symbol}")
            return data
        else:
            print(f"No se encontraron resultados para el símbolo {symbol}")
            return None
    else:
        print(f"Error al obtener datos para {symbol}: {response.status_code}")
        return None


# Se crea un DataFrame principal para almacenar todos los datos
df_principal = pd.DataFrame()

# Se itera sobre los símbolos y se obtienen los datos
for symbol in symbols:
    datos_simbolo = obtener_datos_simbolo(symbol)
    if datos_simbolo is not None:
        if df_principal.empty:
            df_principal = datos_simbolo
        else:
            df_principal = pd.merge(df_principal, datos_simbolo, on="date", how="outer")

# Se muestra el DataFrame combinado
print(df_principal)

# Ahora agregamos los datos por semanas
# Se restablece el índice con la columna 'date' como índice
df_principal.set_index("date", inplace=True)

# Se modifica el dataframe semanalmente, con las funciones de mínimo, máximo, apertura, cierre y suma de volúmenes
df_semanal = df_principal.resample('W').agg({
    **{symbol + '_high': 'max' for symbol in symbols},
    **{symbol + '_low': 'min' for symbol in symbols},
    **{symbol + '_open': 'first' for symbol in symbols},
    **{symbol + '_close': 'last' for symbol in symbols},
    **{symbol + '_volume': 'sum' for symbol in symbols},
})

# Agregar las columnas de mínimo, máximo y volumen combinados de todas las empresas
df_semanal['minimo'] = df_semanal.filter(like='_low').min(axis=1)
df_semanal['maximo'] = df_semanal.filter(like='_high').max(axis=1)
df_semanal['volumen'] = df_semanal.filter(like='_volume').sum(axis=1)

# Se conserva solo las columnas requeridas: mínimo, máximo y volumen
df_semanal = df_semanal[['minimo', 'maximo', 'volumen']]

# Se agrega una nueva columna para el año/semana
df_semanal['year_week'] = df_semanal.index.to_period('W')

# Se reordena las columnas para una mejor presentación
df_semanal = df_semanal.reset_index().set_index('year_week')

# Se muestra el DataFrame con los datos semanales
print("Datos agregados por semana:")
print(df_semanal)

# Ahora agregamos los datos por meses
# Se modifica el dataframe mensualmente y se agregan los datos requeridos
df_mensual = df_principal.resample('ME').agg({
    'AMZN_high': 'max', 'AMZN_low': 'min', 'AMZN_open': 'first', 'AMZN_close': 'last', 'AMZN_volume': 'sum',
    'AAPL_high': 'max', 'AAPL_low': 'min', 'AAPL_open': 'first', 'AAPL_close': 'last', 'AAPL_volume': 'sum',
    'GOOGL_high': 'max', 'GOOGL_low': 'min', 'GOOGL_open': 'first', 'GOOGL_close': 'last', 'GOOGL_volume': 'sum',
    'MSFT_high': 'max', 'MSFT_low': 'min', 'MSFT_open': 'first', 'MSFT_close': 'last', 'MSFT_volume': 'sum',
    'TSLA_high': 'max', 'TSLA_low': 'min', 'TSLA_open': 'first', 'TSLA_close': 'last', 'TSLA_volume': 'sum',
    'NFLX_high': 'max', 'NFLX_low': 'min', 'NFLX_open': 'first', 'NFLX_close': 'last', 'NFLX_volume': 'sum',
    'META_high': 'max', 'META_low': 'min', 'META_open': 'first', 'META_close': 'last', 'META_volume': 'sum',
    'NVDA_high': 'max', 'NVDA_low': 'min', 'NVDA_open': 'first', 'NVDA_close': 'last', 'NVDA_volume': 'sum',
    'SPY_high': 'max', 'SPY_low': 'min', 'SPY_open': 'first', 'SPY_close': 'last', 'SPY_volume': 'sum',
    'V_high': 'max', 'V_low': 'min', 'V_open': 'first', 'V_close': 'last', 'V_volume': 'sum',
})

# Agregar las columnas de mínimo, máximo y volumen combinados de todas las empresas
df_mensual['minimo'] = df_mensual.filter(like='_low').min(axis=1)
df_mensual['maximo'] = df_mensual.filter(like='_high').max(axis=1)
df_mensual['volumen'] = df_mensual.filter(like='_volume').sum(axis=1)

# Se conserva solo las columnas requeridas: mínimo, máximo y volumen
df_mensual = df_mensual[['minimo', 'maximo', 'volumen']]

# Se agrega una nueva columna para el mes
df_mensual['month'] = df_mensual.index.to_period('M')

# Se reordena las columnas para una mejor presentación
df_mensual = df_mensual.reset_index().set_index('month')

# Se muestra el DataFrame con los datos mensuales
print("Datos agregados por mes:")
print(df_mensual)

# Convertir los índices a datetime para evitar problemas de frecuencia
df_semanal.index = df_semanal.index.to_timestamp()
df_mensual.index = df_mensual.index.to_timestamp()

# Unir df_semanal y df_mensual en un solo DataFrame
df_combinado = pd.concat([df_semanal, df_mensual])

# Ordenar por índice (fecha)
df_combinado = df_combinado.sort_index()

# Mostrar el DataFrame final
print("Datos combinados (semanal y mensual):")
print(df_combinado)

