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
# Se utiliza multiprocessing.Queue para que los productos de los bloques sean almacenados y luego sumados.
# Se implementan procesos separados:
# Productor: Calcula productos de bloques y los pone en la cola.
# Consumidor: Suma los bloques del resultado final.

import random
import multiprocessing


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


# Se genera una matriz de bloques (N × N) donde cada bloque es de tamaño (M × M)
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
        fila = [0] * size  # Se inicializa la matriz con ceros
        resultado.append(fila)
    for i in range(size):
        for j in range(size):
            for k in range(size):
                resultado[i][j] += bloque1[i][k] * bloque2[k][j]
    return resultado


# Proceso productor: Calcula productos de bloques y los pone en la cola
def proceso_productor(matriz_a, matriz_b, n, m, queue):
    for i in range(n):
        for j in range(n):
            for k in range(n):
                bloque_producto = multiplicar_bloques(matriz_a[i][k], matriz_b[k][j])
                print(f"Producto de bloques ({i},{k}) y ({k},{j}):")
                for fila in bloque_producto:
                    print(fila)
                queue.put((i, j, bloque_producto))  # Enviar índice y bloque a la cola
    queue.put(None)  # Señal de que el productor ha terminado


# Proceso consumidor: Suma los productos de bloques de la cola
def proceso_consumidor(n, m, queue, matriz_resultado):
    while True:
        item = queue.get()
        if item is None:
            break  # Señal para terminar

        i, j, bloque_producto = item

        # Inicializar el bloque en la matriz resultado si es necesario
        if matriz_resultado[i][j] is None:
            matriz_resultado[i][j] = [[0] * m for _ in range(m)]

        # Sumar los valores del bloque recibido
        for x in range(m):
            for y in range(m):
                matriz_resultado[i][j][x][y] += bloque_producto[x][y]

        # Imprimir matriz resultado intermedia
        print(f"\nMatriz resultado después de sumar bloque ({i},{j}):")
        imprimir_matriz_bloques(matriz_resultado, n, m, "Resultado Intermedio")


# Se imprime una matriz completa de bloques
def imprimir_matriz_bloques(matriz, n, m, nombre="Matriz"):
    print(f"\n{nombre}:")
    matriz_completa = []
    for i in range(n):
        for x in range(m):  # Se itera sobre las filas dentro del bloque
            fila = []
            for j in range(n):
                if matriz[i][j] is not None:  # Asegurarse de que el bloque no sea None
                    fila.extend(matriz[i][j][x])  # Se agrega una fila de cada bloque
            matriz_completa.append(fila)

    for fila in matriz_completa:
        valores_formateados = []
        for num in fila:
            valores_formateados.append(f"{num:.2f}")
        print("  ".join(valores_formateados))


# Función principal para realizar la multiplicación por bloques
def multiplicar_matrices_bloques(matriz_a, matriz_b, n, m):
    queue = multiprocessing.Queue()

    # Inicializar la matriz de resultados con None
    matriz_resultado = [[None for _ in range(n)] for _ in range(n)]

    # Crear y lanzar los procesos
    productor = multiprocessing.Process(target=proceso_productor, args=(matriz_a, matriz_b, n, m, queue))
    consumidor = multiprocessing.Process(target=proceso_consumidor, args=(n, m, queue, matriz_resultado))

    productor.start()
    consumidor.start()

    productor.join()

    # Señal de terminación para el consumidor
    queue.put(None)
    consumidor.join()

    return matriz_resultado


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
