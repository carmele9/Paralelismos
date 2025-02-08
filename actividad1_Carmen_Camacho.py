# UNIVERSITAT CARLEMANY
# Actividad 1: Paralelización, test y profiling de aplicaciones
# Docente Ramon Amela Milian
# Fecha de entrega 10-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 1:
# Programar un algoritmo de multiplicación de matrices cuadradas de igual tamaño. Las
# matrices se generarán de forma aleatoria mediante la función numpy.random.rand. La
# multiplicación de matrices debe realizarse en python puro, sin utilizar las funciones de
# numpy.

from numpy.random import rand


# Se genera una matriz cuadrada de tamaño dado con valores aleatorios
def generar_matriz(size):
    matriz = []
    for i in range(size):
        fila = []
        for j in range(size):
            fila.append(rand())
        matriz.append(fila)
    return matriz


# Se multiplica dos matrices cuadradas
def multiplicar_matrices(matriz1, matriz2):
    size = len(matriz1)
    matriz_result = []
    for i in range(size):
        fila = [0] * size  # Se inicializa la matriz con zeros
        matriz_result.append(fila)
    for i in range(size):
        for j in range(size):
            for k in range(size):
                matriz_result[i][j] += matriz1[i][k] * matriz2[k][j]
    return matriz_result


if __name__ == "__main__":
    size_matriz = 3

    # Se generan matrices aleatorias
    matriz1 = generar_matriz(size_matriz)
    matriz2 = generar_matriz(size_matriz)

    # Se multiplican las matrices
    matriz_resultado = multiplicar_matrices(matriz1, matriz2)

    # Se muestran los resultados
    print("Matriz 1:")
    for fila in matriz1:
        print(fila)
    print("\n Matriz 2:")
    for fila in matriz2:
        print(fila)
    print("\n Resultado de la multiplicación:")
    for fila in matriz_resultado:
        print(fila)
