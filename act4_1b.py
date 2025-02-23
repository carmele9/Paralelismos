# UNIVERSITAT CARLEMANY
# Actividad 4: Utilización de la librería multiprocessing
# Docente Ramon Amela Milian
# Fecha de entrega 24-02-2025
# Carmen De Los Ángeles Camacho Tejada
# Josep Garrido Segues
# Andrei Sevcisen Badarau

# EJERCICIO 1.b:
# Modificar el programa de multiplicación de matrices para que todas las funciones
# paralelas sean ejecutadas en un proceso.
# b. Implementar una versión donde las multiplicaciones de bloques que tienen que
# ser sumadas para obtener el resultado se pongan en sendas colas que serán
# consumidas para realizar las sumas y calcular el resultado final.

# Para esta actividad:
# Cada bloque de la matriz se multiplica en un proceso independiente y se almacena en
# una estructura de datos compartida para evitar conflictos entre procesos.
# Se divide la matriz en bloques de tamaño `M × M`.
# Cada bloque se almacena en una estructura de datos bidimensional.
# Se crean procesos (`multiprocessing.Process`) para calcular el producto de los bloques correspondientes.
# Cada proceso recibe un bloque de la matriz A y un bloque de la matriz B y calcula su producto.
# El resultado se almacena en una estructura de datos compartida (`multiprocessing.Array`).
# Se usa `multiprocessing.Array(ctypes.c_double)` para almacenar la matriz con el resultado en un array plano compartido
# Se sincroniza el acceso con `multiprocessing.Lock()` para evitar condiciones de carrera.
# Se intentó utilizar `multiprocessing.Queue()` para la comunicación entre procesos pero:
# Se detectaron bloqueos debido a la naturaleza síncrona del consumidor, lo que generaba cuellos de botella
# En algunos casos, los bloques de resultado no se almacenaban correctamente, generando matrices vacías.
# Finalmente, la constante transferencia de datos entre procesos usando la cola reducía el rendimiento.


import random
import multiprocessing
import ctypes

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


def multiplicar_bloques(bloque1, bloque2):
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


def worker_multiplicar(i, j, k, bloque_a, bloque_b, resultado_matrices, lock, n, m):
    bloque_producto = multiplicar_bloques(bloque_a, bloque_b)

    with lock:  # Bloqueo para evitar condiciones de carrera
        for x in range(m):
            for y in range(m):
                resultado_matrices[(i * n + j) * (m * m) + (x * m + y)] += bloque_producto[x][y]


# Se multiplican las matrices divididas en bloques de tamaño M × M en paralelo
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m):
    # Se crea un array compartido para la matriz resultado
    resultado_matrices = multiprocessing.Array(ctypes.c_double, n * n * m * m, lock=False)
    lock = multiprocessing.Lock()
    procesos = []

    for i in range(n):
        for j in range(n):
            for k in range(n):
                p = multiprocessing.Process(
                    target=worker_multiplicar,
                    args=(i, j, k, matriz_a[i][k], matriz_b[k][j], resultado_matrices, lock, n, m)
                )
                procesos.append(p)
                p.start()

    for p in procesos:
        p.join()

    # Se convierte el array plano de vuelta a una matriz de bloques
    resultado_final = [[[[
        resultado_matrices[(i * n + j) * (m * m) + (x * m + y)]
        for y in range(m)] for x in range(m)]
        for j in range(n)] for i in range(n)]

    return resultado_final

# Se imprime una matriz completa de bloques
def imprimir_matriz_bloques(matriz, n, m, nombre="Matriz"):
    print(f"\n{nombre}:")
    matriz_completa = []
    for i in range(n):
        for x in range(m):
            fila = []
            for j in range(n):
                fila.extend(matriz[i][j][x])
            matriz_completa.append(fila)
    for fila in matriz_completa:
        print("  ".join(f"{num:.2f}" for num in fila))


#  Se ejecuta el programa
if __name__ == "__main__":
    N = 2  # Cantidad de bloques por fila/columna
    M = 3  # Tamaño de cada bloque

    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M)

    imprimir_matriz_bloques(matriz_A, N, M, "Matriz A")
    imprimir_matriz_bloques(matriz_B, N, M, "Matriz B")
    imprimir_matriz_bloques(resultado, N, M, "Matriz Resultado")
