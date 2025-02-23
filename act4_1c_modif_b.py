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
    bloque1, bloque2 = args
    size = len(bloque1)

    resultado = []
    for _ in range(size):
        fila = [0] * size
        resultado.append(fila)

    for i in range(size):
        for j in range(size):
            for k in range(size):
                resultado[i][j] += bloque1[i][k] * bloque2[k][j]

    return resultado


# Se multiplican las matrices divididas en bloques de tamaño M × M en paralelo
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m):
    matriz_resultado = []
    for _ in range(n):
        fila = [None] * n
        matriz_resultado.append(fila)

    tareas = []
    for i in range(n):
        for j in range(n):
            for k in range(n):
                tarea = (matriz_a[i][k], matriz_b[k][j])
                tareas.append(tarea)

    # Se ejecuta la multiplicación en paralelo
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        resultados = pool.map(multiplicar_bloques, tareas)

    index = 0
    for i in range(n):
        for j in range(n):
            if matriz_resultado[i][j] is None:
                bloque = []
                for _ in range(m):
                    fila = [0] * m
                    bloque.append(fila)
                matriz_resultado[i][j] = bloque

            for x in range(m):
                for y in range(m):
                    matriz_resultado[i][j][x][y] += resultados[index][x][y]

            index += 1

    return matriz_resultado

# Función para medir el tiempo de ejecución
def medir_tiempo_ejecucion(N, M):
    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    start_time = time.time()
    resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M)
    end_time = time.time()

    return end_time - start_time

# Estudio de tiempos para distintas configuraciones
if __name__ == "__main__":
    matriz_tamano = 2000  # Tamaño de la matriz total
    bloques_lista = [1, 2, 4, 8, 16]  # Diferentes cantidades de chunks (bloques)
    for bloques in bloques_lista:
        M = matriz_tamano // bloques  # Tamaño del bloque
        N = bloques  # Número de bloques por fila/columna
        tiempo = medir_tiempo_ejecucion(N, M)
        print(f"Tiempo de ejecución con {bloques} bloques (cada uno de tamaño {M}x{M}): {tiempo:.4f} segundos")
