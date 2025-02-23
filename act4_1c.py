# UNIVERSITAT CARLEMANY
# Actividad 4: Utilización de la librería multiprocessing
# Docente Ramon Amela Milian
# Fecha de entrega 24-02-2025
# Carmen De Los Ángeles Camacho Tejada
# Josep Garrido Segues
# Andrei Sevcisen Badarau

# EJERCICIO 1.c:
# Modificar el programa de multiplicación de matrices para que todas las funciones
# paralelas sean ejecutadas en un proceso.
# Modificar la versión a) para utilizar Pools.


# Para esta actividad:
# Usamos multiprocessing.Pool.map, donde cada bloque se calcula de forma independiente:
# No usamos sincronización manual, porque cada bloque es independiente de los demás.
# Uso de cpu_count() para elegir el número de procesos óptimo:
# Esto garantiza que el código se ejecuta eficientemente en cualquier hardware.
# Optimización del manejo de resultados:
# Cada resultado se guarda en matriz_resultado[i][j] de forma acumulativa.

import random
import multiprocessing


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

    # Se preparan los argumentos para la multiplicación en paralelo
    tareas = []
    for i in range(n):
        for j in range(n):
            for k in range(n):
                tarea = (matriz_a[i][k], matriz_b[k][j])  # Tupla de bloques a multiplicar
                tareas.append(tarea)

    # Se ejecuta la multiplicación en paralelo
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        resultados = pool.map(multiplicar_bloques, tareas)

    # Se construye la matriz con el resultado a partir de los resultados
    index = 0
    for i in range(n):
        for j in range(n):
            if matriz_resultado[i][j] is None:
                bloque = []
                for _ in range(m):
                    fila = [0] * m
                    bloque.append(fila)
                matriz_resultado[i][j] = bloque

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
        valores_formateados = []
        for num in fila:
            valores_formateados.append(f"{num:.2f}")
        print("  ".join(valores_formateados))


if __name__ == "__main__":
    N = 2  # Cantidad de bloques por fila/columna
    M = 3  # Tamaño de cada bloque

    # Se generan las matrices de bloques
    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    # Se multiplican las matrices por bloques en paralelo
    resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M)

    # Se muestran los resultados
    imprimir_matriz_bloques(matriz_A, N, M, "Matriz A")
    imprimir_matriz_bloques(matriz_B, N, M, "Matriz B")
    imprimir_matriz_bloques(resultado, N, M, "Matriz Resultado")
