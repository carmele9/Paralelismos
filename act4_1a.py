# UNIVERSITAT CARLEMANY
# Actividad 4: Utilización de la librería multiprocessing
# Docente Ramon Amela Milian
# Fecha de entrega 24-02-2025
# Carmen De Los Ángeles Camacho Tejada
# Josep Garrido Segues
# Andrei Sevcisen Badarau

# EJERCICIO 1.a:
# Modificar el programa de multiplicación de matrices para que todas las funciones
# paralelas sean ejecutadas en un proceso.
# a. Implementar una versión donde no haga falta coordinación porque cada bloque
# de salida se calcule en una función.

# Para esta actividad:
# Procesos individuales con multiprocessing.Process:
# Se crea un proceso para cada tarea de multiplicación de bloques.
# Cada proceso se ejecuta de manera independiente y paralela.
# proceso.start(): Se inicia cada proceso.
# proceso.join(): Espera a que todos los procesos terminen antes de continuar,
# asegurando que el cálculo esté completo antes de imprimir los resultados.
# Uso de multiprocessing.Manager: Se crea una lista gestionada que se compartirá entre los procesos.
# Esto permite que cada proceso pueda actualizar la matriz de resultados sin que se produzcan problemas de concurrencia.
# La matriz de resultados se genera usando manager.list()
# para asegurar que sea accesible por todos los procesos de manera segura.
# Al final de la función multiplicar_matrices_bloques, se convierte la lista gestionada por Manager en una lista normal
# para que sea más fácil trabajar con ella.

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
def multiplicar_bloques(bloque1, bloque2, resultado, i, j):
    size = len(bloque1)

    # Se inicializa la matriz con ceros
    bloque_resultado = []
    for _ in range(size):
        fila = [0] * size
        bloque_resultado.append(fila)

    for x in range(size):
        for y in range(size):
            for z in range(size):
                bloque_resultado[x][y] += bloque1[x][z] * bloque2[z][y]

    # Se asigna el resultado de la multiplicación al bloque correspondiente en la matriz resultado
    resultado[i][j] = bloque_resultado


# Se multiplican las matrices divididas en bloques de tamaño M × M en paralelo
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m):
    # Se crea un Manager para compartir los datos entre procesos
    with multiprocessing.Manager() as manager:
        # Se crea la matriz resultado compartida
        matriz_resultado = manager.list([manager.list([None] * n) for _ in range(n)])

        # Lista de procesos que se van a ejecutar en paralelo
        procesos = []

        for i in range(n):
            for j in range(n):
                for k in range(n):
                    # Se crea un nuevo proceso para cada tarea de multiplicación de bloques
                    proceso = multiprocessing.Process(target=multiplicar_bloques,
                                                      args=(matriz_a[i][k], matriz_b[k][j], matriz_resultado, i, j))
                    procesos.append(proceso)
                    proceso.start()

        # Se espera a que todos los procesos terminen
        for proceso in procesos:
            proceso.join()

        # Se convierte la lista gestionada por el Manager en una lista normal para trabajar con ella
        return [list(fila) for fila in matriz_resultado]


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

    # Se multiplican las matrices por bloques en paralelo sin Pool
    resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M)

    # Se muestran los resultados
    imprimir_matriz_bloques(matriz_A, N, M, "Matriz A")
    imprimir_matriz_bloques(matriz_B, N, M, "Matriz B")
    imprimir_matriz_bloques(resultado, N, M, "Matriz Resultado")
