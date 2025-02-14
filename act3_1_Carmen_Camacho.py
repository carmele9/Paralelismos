# UNIVERSITAT CARLEMANY
# Actividad 3: Utilización de la libreria threading
# Docente Ramon Amela Milian
# Fecha de entrega 17-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 1:
# Modificar el programa de multiplicación de matrices para que todas las funciones
# paralelas sean ejecutadas en un thread.
# Implementar una versión donde se hagan todas las multiplicaciones primero y luego se
# realicen las sumas de los diferentes bloques en funciones diferentes.

# Para esta actividad:
# Primero se realizan todas las multiplicaciones en hilos (multiplicar_bloques)
# y luego, en hilos separados, se realizan las sumas de los resultados (sumar_bloques).


import threading
import numpy as np


# Se generan los bloques
def generar_bloque(m, min_val=0, max_val=10):
    return np.random.uniform(min_val, max_val, (m, m))


# Se generan las matrices por bloques
def generar_matriz_bloques(n, m):
    matriz = []
    for i in range(n):
        fila = []
        for j in range(n):
            fila.append(generar_bloque(m))
        matriz.append(fila)
    return matriz


# Se multiplican por bloques
def multiplicar_bloques(bloque1, bloque2, resultado, i, j):
    # En la actividad 1 no use este np.dot(), sin embargo, me salía error y solo asi pude corregirlo.
    # Dado que la actividad se basa en el uso de la libreria threading, he decidido mantenerlo
    resultado[i][j] = np.dot(bloque1, bloque2)


# Se suman los bloques
def sumar_bloques(bloques, resultado, i, j):
    # Verificación si son matrices o números
    if isinstance(bloques, np.ndarray):
        resultado[i][j] = bloques
    elif isinstance(bloques, list):
        resultado[i][j] = sum(bloques)
    else:
        raise TypeError(f"Tipo inesperado en bloques: {type(bloques)}")


# Se multiplican las matrices usando threading
def multiplicar_matrices_threading(matriz_a, matriz_b, n, m):
    # Se inicializa la matriz 'multiplicaciones'
    multiplicaciones = []
    for i in range(n):
        fila = []
        for j in range(n):
            fila.append([])
        multiplicaciones.append(fila)

    # Se inicializa la matriz 'suma_resultados' con matrices de ceros de numpy
    # En la actividad 1, tampoco use esta función
    suma_resultados = []
    for i in range(n):
        fila = []
        for j in range(n):
            bloque = np.zeros((m, m))
            fila.append(bloque)
        suma_resultados.append(fila)

    # Multiplicación de bloques
    threads = []
    for i in range(n):
        for j in range(n):
            for k in range(n):
                hilo = threading.Thread(
                    target=multiplicar_bloques,
                    args=(matriz_a[i][k], matriz_b[k][j], multiplicaciones, i, j)
                )
                threads.append(hilo)
                hilo.start()

    for hilo in threads:
        hilo.join()

    # Suma de bloques
    suma_threads = []
    for i in range(n):
        for j in range(n):
            hilo = threading.Thread(
                target=sumar_bloques,
                args=(multiplicaciones[i][j], suma_resultados, i, j)
            )
            suma_threads.append(hilo)
            hilo.start()

    for hilo in suma_threads:
        hilo.join()

    return suma_resultados


# Se imprimen las matrices
def imprimir_matriz_bloques(matriz, n, m, nombre="Matriz"):
    print(f"\n{nombre}:")
    for i in range(n):
        for x in range(m):
            fila = []
            for j in range(n):
                bloque = matriz[i][j]
                fila.extend(bloque[x])
            print("  ".join(f"{num:.2f}" for num in fila))


# Se ejecuta el programa
if __name__ == "__main__":
    N = 2  # Cantidad de bloques por fila/columna
    M = 3  # Tamaño de cada bloque

    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    resultado = multiplicar_matrices_threading(matriz_A, matriz_B, N, M)

    imprimir_matriz_bloques(matriz_A, N, M, "Matriz A")
    imprimir_matriz_bloques(matriz_B, N, M, "Matriz B")
    imprimir_matriz_bloques(resultado, N, M, "Matriz Resultado")
