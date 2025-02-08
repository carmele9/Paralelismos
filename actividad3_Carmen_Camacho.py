# UNIVERSITAT CARLEMANY
# Actividad 1: Paralelización, test y profiling de aplicaciones
# Docente Ramon Amela Milian
# Fecha de entrega 10-02-2025
# Carmen De Los Ángeles Camacho Tejada

# EJERCICIO 3:
# Realizar un test para verificar que los resultados de los algoritmos 1 y 2 son iguales


import actividad2_Carmen_Camacho as act2
import numpy as np


# Se realiza el test para ver que ambas multiplicaciones son iguales
def test_multiplicacion_matrices():
    N = 2  # Cantidad de bloques por fila/columna
    M = 3  # Tamaño de cada bloque

    # Se generan matrices aleatorias en forma estándar y en bloques
    matriz1_bloques = act2.generar_matriz_bloques(N, M)
    matriz2_bloques = act2.generar_matriz_bloques(N, M)

    # Se multiplican usando el algoritmo por bloques
    resultado_bloques = np.array(act2.multiplicar_matrices_bloques(matriz1_bloques, matriz2_bloques, N, M))

    # Se convierten las matrices de bloques en matrices enteras usando np.block
    resultado_bloques_completo = np.block([
        [np.block(row) for row in resultado_bloques]
    ])
    matriz1_completa = np.block([
        [np.block(row) for row in matriz1_bloques]  # Aplanar cada fila de bloques
    ])
    matriz2_completa = np.block([
        [np.block(row) for row in matriz2_bloques]  # Aplanar cada fila de bloques
    ])

    # Se usa np.dot para comparar con la multiplicación tradicional
    # Primero, se deben reorganizar las dimensiones para que sean compatibles
    matriz1_completa = matriz1_completa.reshape(-1, M * N)
    matriz2_completa = matriz2_completa.reshape(M * N, -1)
    resultado_numpy = np.dot(matriz1_completa, matriz2_completa)

    # Se aplanan ambas matrices para que tengan una forma compatible
    resultado_bloques_completo = resultado_bloques_completo.reshape(-1)
    resultado_numpy = resultado_numpy.reshape(-1)

    # Se comparan las matrices de los resultados
    for i in range(len(resultado_numpy)):
        if round(resultado_numpy[i], 6) != round(resultado_bloques_completo[i], 6):
            print(f"ERROR: Diferencia en posición {i}")
            print(f"Algoritmo Estándar: {resultado_numpy[i]}")
            print(f"Algoritmo Bloques:  {resultado_bloques_completo[i]}")
            return False

    print("TEST PASADO CON ÉXITO: Ambos algoritmos producen el mismo resultado.")
    return True


if __name__ == "__main__":
    test_multiplicacion_matrices()
