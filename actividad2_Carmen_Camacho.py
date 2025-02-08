# UNIVERSITAT CARLEMANY
# Actividad 1: Paralelización, test y profiling de aplicaciones
# Docente Ramon Amela Milian
# Fecha de entrega 10-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 2:
# Realizar una versión por bloques del anterior algoritmo. Las matrices se deben generar
# teniendo en cuenta dos parámetros: “cantidad de bloques” (N) y “medida de los
# bloques” (M). Siguiendo esta lógica, cada matriz estará compuesta por NxN bloques de MxM unidades.
# Cada operación que se desee ejecutar en paralelo debe ejecutarse
# dentro de una función. Estos parámetros han de ser fácilmente configurables

import random


# Se genera un bloque de tamaño M × M con valores aleatorios
def generar_bloque(m, min_val=0, max_val=10):
    matriz = []
    for i in range(m):
        fila = []
        for j in range(m):
            valor = random.uniform(min_val, max_val)
            fila.append(valor)
        matriz.append(fila)
    return matriz


#  Se genera una matriz de bloques (N × N) donde cada bloque es de tamaño (M × M)
def generar_matriz_bloques(n, m):
    matriz = []
    for i in range(n):
        fila = []
        for j in range(n):
            fila.append(generar_bloque(m))
        matriz.append(fila)
    return matriz


# Se multiplican los dos bloques M × M mediante la multiplicación tradicional
def multiplicar_bloques(bloque1, bloque2):
    size = len(bloque1)
    resultado = []
    for i in range(size):
        fila = [0] * size  # Se inicializa la matriz con zeros
        resultado.append(fila)
    for i in range(size):
        for j in range(size):
            for k in range(size):
                resultado[i][j] += bloque1[i][k] * bloque2[k][j]
    return resultado


# Se multiplican las matrices divididas en bloques de tamaño M × M.
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m):
    matriz_resultado = []
    # Se genera por bloques la matriz con el resultado inicializándola a zero
    for i in range(n):
        fila_bloques = []
        for j in range(n):
            bloque = []
            for x in range(m):
                fila_bloque = [0] * m
                bloque.append(fila_bloque)
            fila_bloques.append(bloque)
        matriz_resultado.append(fila_bloques)
    for i in range(n):
        for j in range(n):
            bloque_resultado = []
            # Se crea la matriz bloqueo_resultado
            for z in range(m):
                fila = []
                for y in range(m):
                    fila.append(0)
                bloque_resultado.append(fila)
            for k in range(n):
                bloque_producto = multiplicar_bloques(matriz_a[i][k], matriz_b[k][j])
                # Se suma al bloque resultado
                for x in range(m):
                    for y in range(m):
                        bloque_resultado[x][y] += bloque_producto[x][y]
            matriz_resultado[i][j] = bloque_resultado
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

    # Se generan las matrices de bloques
    matriz_A = generar_matriz_bloques(N, M)
    matriz_B = generar_matriz_bloques(N, M)

    # Se multiplican las matrices por bloques
    resultado = multiplicar_matrices_bloques(matriz_A, matriz_B, N, M)

    # Se muestran los resultados
    imprimir_matriz_bloques(matriz_A, N, M, "Matriz A")
    imprimir_matriz_bloques(matriz_B, N, M, "Matriz B")
    imprimir_matriz_bloques(resultado, N, M, "Matriz Resultado")
