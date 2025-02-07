import numpy as np
from numpy.random import rand
from numpy.random import seed

np.set_printoptions(formatter={'float': lambda x: "{0:0.3f}".format(x)})

def divide_vector(vector):
    vector_dividido = []
    for i in range(int(len(vector) / 10)):
        vector_dividido.append(vector[i * 10:(i+1)*10])
    return vector_dividido

def suma_i(vector, posicion, suma_base):
    vector[posicion] = vector[posicion] + posicion + suma_base


def incrementa_particion(parte, suma_base):
    for i in range(len(parte)):
        suma_i(parte, i, suma_base)


def multiplicar_componentes(matriz_1, matriz_2):
    matriz_resultado = []
    for fila in range(len(matriz_1)):
        matriz_resultado.append([])
        for columna in range(len(matriz_1[fila])):
            matriz_resultado[fila].append(matriz_1[fila][columna] * matriz_2[fila][columna])
    return np.array(matriz_resultado)


def multiplicaciones_bloques(matriz_a, matriz_b):
    matriz_resultado = []
    for fila in range(len(matriz_a)):
        matriz_resultado.append([])
        for columna in range(len(matriz_a[0])):
            # Componente fila, columna
            matriz_resultado[fila].append(
                multiplicar_componentes(matriz_a[fila][columna], matriz_b[fila][columna])
            )


if __name__ == '__main__':
    vector_original = 1000 * [1]
    copia_vector = vector_original.copy()

    for i in range(len(vector_original)):
        suma_i(vector_original, i, 0)

    vector_dividido = divide_vector(copia_vector)


    for i in range(len(vector_dividido)):
        suma_base = 10 * i
        incrementa_particion(vector_dividido[i], suma_base)

    vector_resultado = []
    for parte in vector_dividido:
        vector_resultado.extend(parte)

    # print(vector_original)
    # print(vector_resultado)
    # print("Los vectores son iguales?  " + str(vector_original == vector_resultado))

    numero_bloques= 3
    tamano_bloque = 2

    matriz_a = []
    matriz_b = []

    seed(10)

    for fila in range(numero_bloques):
        matriz_a.append([])
        matriz_b.append([])
        for columna in range(numero_bloques):
            matriz_a[fila].append(rand(tamano_bloque, tamano_bloque))
            matriz_b[fila].append(rand(tamano_bloque, tamano_bloque))

    matriz_resultado = multiplicaciones_bloques(matriz_a, matriz_b)

    matriz_a_junta = np.block(matriz_a)
    matriz_b_junta = np.block(matriz_b)
    print(matriz_a_junta)

    print(np.multiply(matriz_a_junta, matriz_b_junta))
    print(np.block(matriz_resultado))
    print(np.allclose(
        np.block(matriz_resultado), np.multiply(matriz_a_junta, matriz_b_junta))
    )
    # print(np.dot(matriz_a, matriz_b))
