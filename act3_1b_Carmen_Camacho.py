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
# Dado que no sabía en el momento de realizar la actividad
# si se referia a multiplicacion tradicional o bloques, hice las dos.

from numpy.random import rand
import threading

# Se genera una matriz cuadrada de tamaño dado con valores aleatorios
def generar_matriz(size):
    matriz = []
    for i in range(size):
        fila = []
        for j in range(size):
            fila.append(rand())
        matriz.append(fila)
    return matriz

# Se realiza una función para multiplicar un bloque de matrices
def multiplicar_bloque(matriz1, matriz2, resultado, i, j):
    size = len(matriz1)
    bloque = 0
    for k in range(size):
        bloque += matriz1[i][k] * matriz2[k][j]
    resultado[i][j] = bloque

# Se realiza una función para sumar bloques (en este caso es la suma final de todos los resultados)
def sumar_bloques(resultados_parciales, resultado_final):
    for i in range(len(resultados_parciales)):
        for j in range(len(resultados_parciales[0])):
            resultado_final[i][j] += resultados_parciales[i][j]

# Se multiplican las matrices usando threading
def multiplicar_matrices_threading(matriz1, matriz2):
    size = len(matriz1)
    resultado_parcial = [[0] * size for _ in range(size)]
    threads = []

    # Se crean los hilos para la multiplicación
    for i in range(size):
        for j in range(size):
            hilo = threading.Thread(target=multiplicar_bloque, args=(matriz1, matriz2, resultado_parcial, i, j))
            threads.append(hilo)
            hilo.start()

    for hilo in threads:
        hilo.join()

    # Se suma en un solo hilo para evitar concurrencia en la suma
    resultado_final = [[0] * size for _ in range(size)]
    hilo_suma = threading.Thread(target=sumar_bloques, args=(resultado_parcial, resultado_final))
    hilo_suma.start()
    hilo_suma.join()

    return resultado_final


# Se ejecuta el programa
if __name__ == "__main__":
    size_matriz = 3

    matriz1 = generar_matriz(size_matriz)
    matriz2 = generar_matriz(size_matriz)

    matriz_resultado = multiplicar_matrices_threading(matriz1, matriz2)

    print("Matriz 1:")
    for fila in matriz1:
        print(fila)
    print("\nMatriz 2:")
    for fila in matriz2:
        print(fila)
    print("\nResultado de la multiplicación:")
    for fila in matriz_resultado:
        print(fila)
