# UNIVERSITAT CARLEMANY
# Actividad 4: Utilización de la librería multiprocessing
# Docente Ramon Amela Milian
# Fecha de entrega 24-02-2025
# Carmen De Los Ángeles Camacho Tejada
# Josep Garrido Segues
# Andrei Sevcisen Badarau

# EJERCICIO 1.c-Modificado para realizar los estudios de viabilidad:
# a. Realizar un estudio analizando la variabilidad del tiempo de ejecución en
# función del número de procesos indicados en el Pool. Se debe llegar, como
# mínimo, un número de procesos igual a 7 veces el número de procesadores
# disponibles en la máquina donde se ejecute el experimento. Comentar los
# resultados obtenidos.


import random
import multiprocessing
import time


# Se genera un bloque de tamaño M × M con valores aleatorios
def generar_bloque(m, min_val=0, max_val=10):
    matriz = []
    for _ in range(m):
        fila = []
        for _ in range(m):
            valor = random.uniform(min_val, max_val)
            fila.append(valor)
        matriz.append(fila)
    return matriz


# Se genera una matriz de bloques (N × N) donde cada bloque es de tamaño (M × M)
def generar_matriz_bloques(n, m):
    matriz = []
    for _ in range(n):
        fila = []
        for _ in range(n):
            bloque = generar_bloque(m)
            fila.append(bloque)
        matriz.append(fila)
    return matriz


# Se multiplican los dos bloques M × M mediante la multiplicación tradicional
def multiplicar_bloques(args):
    bloque1, bloque2 = args  # Se reciben los bloques como una tupla
    size = len(bloque1)

    # Se inicializa la matriz con ceros
    resultado = [[0] * size for _ in range(size)]

    for i in range(size):
        for j in range(size):
            for k in range(size):
                resultado[i][j] += bloque1[i][k] * bloque2[k][j]

    return resultado


# Se multiplican las matrices divididas en bloques de tamaño M × M en paralelo
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m, num_procesos):
    matriz_resultado = [[None] * n for _ in range(n)]

    # Se preparan los argumentos para la multiplicación en paralelo
    tareas = [(matriz_a[i][k], matriz_b[k][j]) for i in range(n) for j in range(n) for k in range(n)]

    # Se ejecuta la multiplicación en paralelo con un número variable de procesos
    with multiprocessing.Pool(processes=num_procesos) as pool:
        resultados = pool.map(multiplicar_bloques, tareas)

    # Se construye la matriz con el resultado a partir de los resultados
    index = 0
    for i in range(n):
        for j in range(n):
            if matriz_resultado[i][j] is None:
                matriz_resultado[i][j] = [[0] * m for _ in range(m)]

            # Se suman los bloques que corresponden a cada posición (i, j)
            for x in range(m):
                for y in range(m):
                    matriz_resultado[i][j][x][y] += resultados[index][x][y]

            index += 1

    return matriz_resultado


#  Se imprime una matriz completa de bloques
def imprimir_matriz_bloques(matriz, n, m, nombre="Matriz"):
    print(f"\n{nombre}:")
    matriz_completa = []

    for i in range(n):
        for x in range(m):  # Se itera sobre las filas dentro del bloque
            fila = []
            for j in range(n):
                fila.extend(matriz[i][j][x])  # Se agrega una fila de cada bloque
            matriz_completa.append(fila)

    for fila in matriz_completa:
        print("  ".join(f"{num:.2f}" for num in fila))


if __name__ == "__main__":
    N = 2  # Cantidad de bloques por fila/columna
    M = 3  # Tamaño de cada bloque

    # Se genera una matriz de bloques aleatoria
    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    # Número de procesadores disponibles
    num_cpu = multiprocessing.cpu_count()

    # Se probarán estos valores de número de procesos
    lista_procesos = [1, 2, 4, 8, 16, 32, 64, num_cpu * 7]

    for num_procesos in lista_procesos:
        print(f"\nEjecutando con {num_procesos} procesos...")
        # Medir el tiempo de ejecución
        tiempo_inicio = time.perf_counter()
        resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M, num_procesos)
        tiempo_fin = time.perf_counter()

        tiempo_total = tiempo_fin - tiempo_inicio
        print(f"Tiempo de ejecución: {tiempo_total:.4f} segundos")

