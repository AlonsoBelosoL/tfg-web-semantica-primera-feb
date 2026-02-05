import pandas as pd # Importamos la librería pandas para manejar las tablas de datos
import os # Importamos os para gestionar las carpetas de tu ordenador
import numpy as np # Importamos numpy para realizar operaciones matemáticas avanzadas

def ejecutar_procesamiento_capa_3(): # Función principal para calcular estadísticas avanzadas
    print("Iniciando Capa 3: Generacion de Analitica Avanzada de Jugadores y Equipos...") # Mensaje de inicio

    # --- CONFIGURACIÓN DE RUTAS ---
    CARPETA_CAPA1 = 'datos/procesados/capa1/' # Ruta de los datos maestros
    CARPETA_CAPA2 = 'datos/procesados/capa2/' # Ruta de los datos limpios de la capa anterior
    CARPETA_CAPA3 = 'datos/procesados/capa3/' # Ruta donde guardaremos los resultados finales
    os.makedirs(CARPETA_CAPA3, exist_ok=True) # Creamos la carpeta de la capa 3 si no existe

    try: # Intentamos cargar todos los archivos necesarios
        tabla_maestra_jugadores = pd.read_csv(os.path.join(CARPETA_CAPA1, 'capa1_jugadores.csv')) # Cargamos nombres y URLs de jugadores
        tabla_maestra_equipos = pd.read_csv(os.path.join(CARPETA_CAPA1, 'capa1_equipos.csv')) # Cargamos información de equipos
        tabla_detallada = pd.read_csv(os.path.join(CARPETA_CAPA2, 'capa2_estadisticas_detalladas.csv')) # Cargamos estadísticas partido a partido
        tabla_partidos = pd.read_csv(os.path.join(CARPETA_CAPA2, 'capa2_partidos.csv')) # Cargamos los resultados de los partidos
    except FileNotFoundError as error: # Si falta algún archivo
        print(f"Error: No se han encontrado los archivos de las capas anteriores. {error}") # Avisamos del error
        return # Frenamos el programa

    # --- LIMPIEZA Y PREPARACIÓN DE DATOS ---
    
    def convertir_minutos_a_decimal(tiempo): # Función para pasar minutos de formato "20:30" a "20.5"
        if pd.isna(tiempo): return 0.0 # Si no hay tiempo, devolvemos cero
        if isinstance(tiempo, str) and ':' in tiempo: # Si el texto tiene dos puntos (formato minutos:segundos)
            partes = tiempo.split(':') # Dividimos el texto por los dos puntos
            return float(partes[0]) + float(partes[1])/60 # Sumamos los minutos y la parte proporcional de los segundos
        return float(tiempo) # Si ya es un número, lo devolvemos tal cual
    
    tabla_detallada['minutos_decimal'] = tabla_detallada['minutos'].apply(convertir_minutos_a_decimal) # Aplicamos la conversión de minutos
    
    # Lista de columnas que deben ser números para poder sumarlas
    columnas_a_limpiar = ['puntos', 'valoracion', 'asistencias', 'robos', 'perdidas', 'tapones', 
                          'rebotes_ofensivos', 'rebotes_defensivos', 'rebotes_totales', 
                          'faltas_cometidas', 'faltas_recibidas', 'mas_menos']
    
    for columna in columnas_a_limpiar: # Recorremos cada columna de la lista
        if columna in tabla_detallada.columns: # Si la columna existe en nuestra tabla
            tabla_detallada[columna] = tabla_detallada[columna].fillna(0) # Cambiamos los valores vacíos por un cero
        else: # Si la columna no existe por algún motivo
            tabla_detallada[columna] = 0 # La creamos rellena de ceros

    # Calculamos el total de tiros de campo (sumando tiros de 2 y de 3 puntos)
    tabla_detallada['tiros_campo_metidos'] = tabla_detallada['t2_metidos'] + tabla_detallada['t3_metidos']
    tabla_detallada['tiros_campo_intentados'] = tabla_detallada['t2_intentados'] + tabla_detallada['t3_intentados']

    def calcular_doble_doble(fila): # Función para saber si un jugador logró un doble-doble
        # Contamos en cuántas categorías principales el jugador llegó a 10 o más
        categorias = sum(1 for valor in [fila['puntos'], fila['rebotes_totales'], fila['asistencias'], fila['robos'], fila['tapones']] if valor >= 10)
        return 1 if categorias >= 2 else 0 # Si llegó a 10 en 2 o más categorías, es un doble-doble
    
    tabla_detallada['es_doble_doble'] = tabla_detallada.apply(calcular_doble_doble, axis=1) # Aplicamos la función a cada partido

    # --- ANALÍTICA DE JUGADORES (RESUMEN POR TEMPORADA) ---
    print("Calculando promedios y totales de los jugadores...") # Mensaje de progreso
    
    # Agrupamos los datos por jugador, equipo y año
    agrupado_jugadores = tabla_detallada.groupby(['url_jugador', 'uri_equipo', 'ano_inicio'])
    
    # Definimos qué queremos hacer con cada dato (sumar totales o calcular promedios)
    operaciones_jugador = {
        'minutos_decimal': ['sum', 'mean'],
        'puntos': ['sum', 'mean', 'count'], # El 'count' nos dirá cuántos partidos jugó
        'valoracion': ['sum', 'mean'],
        'asistencias': ['sum', 'mean'],
        'robos': ['sum', 'mean'],
        'perdidas': ['sum', 'mean'],
        'tapones': ['sum', 'mean'],
        'rebotes_ofensivos': ['sum', 'mean'],
        'rebotes_defensivos': ['sum', 'mean'],
        'rebotes_totales': ['sum', 'mean'],
        'faltas_cometidas': ['sum', 'mean'],
        'faltas_recibidas': ['sum', 'mean'],
        'mas_menos': ['sum', 'mean'],
        'es_doble_doble': 'sum',
        't2_metidos': 'sum', 't2_intentados': 'sum',
        't3_metidos': 'sum', 't3_intentados': 'sum',
        't1_metidos': 'sum', 't1_intentados': 'sum',
        'tiros_campo_metidos': 'sum', 'tiros_campo_intentados': 'sum'
    }

    resultados_jugadores = agrupado_jugadores.agg(operaciones_jugador) # Realizamos los cálculos
    resultados_jugadores.columns = [f"{col[0]}_{col[1]}" for col in resultados_jugadores.columns] # Unimos los nombres de las columnas
    resultados_jugadores = resultados_jugadores.reset_index() # Reorganizamos el índice de la tabla

    # Renombramos las columnas para que sean fáciles de leer
    resultados_jugadores.columns = [c.replace('_sum', '_total').replace('_mean', '_promedio').replace('_count', '_partidos').replace('minutos_decimal', 'minutos') for c in resultados_jugadores.columns]
    resultados_jugadores = resultados_jugadores.rename(columns={'puntos_partidos': 'partidos_jugados'}) # Renombramos la cuenta de partidos
    resultados_jugadores = resultados_jugadores.loc[:, ~resultados_jugadores.columns.str.contains('_partidos')] # Quitamos columnas de cuenta sobrantes

    # --- FÓRMULAS DE ESTADÍSTICA AVANZADA (JUGADORES) ---
    
    # True Shooting %: Mide la eficiencia de tiro teniendo en cuenta triples y tiros libres
    resultados_jugadores['ts_porcentaje'] = (resultados_jugadores['puntos_total'] / (2 * (resultados_jugadores['tiros_campo_intentados_total'] + 0.44 * resultados_jugadores['t1_intentados_total'] + 0.001))) * 100
    
    # Effective Field Goal %: Mide la eficiencia de tiro dando más valor al triple
    resultados_jugadores['efg_porcentaje'] = ((resultados_jugadores['tiros_campo_metidos_total'] + 0.5 * resultados_jugadores['t3_metidos_total']) / (resultados_jugadores['tiros_campo_intentados_total'] + 0.001)) * 100
    
    # Ratio Asistencias/Pérdidas: Cuántas asistencias da el jugador por cada balón que pierde
    resultados_jugadores['ratio_ast_to'] = resultados_jugadores['asistencias_total'] / (resultados_jugadores['perdidas_total'] + 0.001)
    
    # Valoración por minuto: Eficiencia general en relación al tiempo que está en pista
    resultados_jugadores['valoracion_por_minuto'] = resultados_jugadores['valoracion_total'] / (resultados_jugadores['minutos_total'] + 0.001)
    
    # Posesiones Terminadas: Estimación de cuántas posesiones del equipo finaliza este jugador
    resultados_jugadores['posesiones_terminadas'] = resultados_jugadores['tiros_campo_intentados_total'] + 0.44 * resultados_jugadores['t1_intentados_total'] + resultados_jugadores['perdidas_total']
    
    # Rating Ofensivo Individual: Puntos que produciría el jugador si terminara 100 posesiones
    resultados_jugadores['ortg_individual'] = (resultados_jugadores['puntos_total'] / (resultados_jugadores['posesiones_terminadas'] + 0.001)) * 100

    # Combinamos con la tabla maestra para recuperar el nombre real del jugador
    resultados_jugadores_final = resultados_jugadores.merge(tabla_maestra_jugadores, on='url_jugador', how='left')
    # Guardamos los resultados de los jugadores redondeando a 2 decimales
    resultados_jugadores_final.round(2).to_csv(os.path.join(CARPETA_CAPA3, 'capa3_jugadores_avanzado.csv'), index=False)


    # --- ANALÍTICA DE EQUIPOS (RESUMEN POR TEMPORADA) ---
    print("Calculando promedios y totales de los equipos...") # Mensaje de progreso

    # Columnas que vamos a sumar para obtener los totales del equipo en cada partido
    columnas_totales_equipo = ['puntos', 'valoracion', 'asistencias', 'robos', 'perdidas', 'tapones',
                               'rebotes_ofensivos', 'rebotes_defensivos', 'rebotes_totales', 
                               'faltas_cometidas', 'faltas_recibidas', 'mas_menos',
                               't2_metidos', 't2_intentados', 't3_metidos', 't3_intentados', 
                               't1_metidos', 't1_intentados', 'tiros_campo_metidos', 'tiros_campo_intentados']
    
    # Sumamos las estadísticas de todos los jugadores para tener el total del equipo por cada partido
    equipo_por_partido = tabla_detallada.groupby(['id_partido', 'uri_equipo'])[columnas_totales_equipo].sum().reset_index()
    
    # Cruzamos con la tabla de partidos para saber quién ganó y quién perdió
    datos_basicos_partidos = tabla_partidos[['id_partido', 'uri_local', 'uri_visitante', 'puntos_local', 'puntos_visitante', 'ano_inicio']]
    equipo_por_partido = equipo_por_partido.merge(datos_basicos_partidos, on='id_partido')

    def determinar_victoria(fila): # Función para saber si el equipo ganó el partido
        if fila['uri_equipo'] == fila['uri_local']: # Si el equipo jugaba como local
            return 1 if fila['puntos_local'] > fila['puntos_visitante'] else 0 # Gana si metió más puntos que el visitante
        return 1 if fila['puntos_visitante'] > fila['puntos_local'] else 0 # Si era visitante, gana si metió más que el local
    
    equipo_por_partido['victoria'] = equipo_por_partido.apply(determinar_victoria, axis=1) # Aplicamos la lógica de victorias

    # Agrupamos ahora los resultados por equipo y año para el resumen estacional
    agrupado_equipos = equipo_por_partido.groupby(['uri_equipo', 'ano_inicio'])
    
    # Preparamos las operaciones para el equipo (Victorias totales y promedios de juego)
    operaciones_equipo = {'victoria': ['sum', 'count']}
    for col in columnas_totales_equipo:
        operaciones_equipo[col] = ['sum', 'mean']
    
    resultados_equipos = agrupado_equipos.agg(operaciones_equipo) # Realizamos los cálculos
    resultados_equipos.columns = [f"{col[0]}_{col[1]}" for col in resultados_equipos.columns] # Unimos nombres de columnas
    resultados_equipos = resultados_equipos.reset_index() # Reorganizamos el índice

    # Renombramos las columnas para que sean claras
    resultados_equipos.columns = [c.replace('_sum', '_total').replace('_mean', '_promedio').replace('_count', '_partidos') for c in resultados_equipos.columns]
    resultados_equipos = resultados_equipos.rename(columns={'victoria_partidos': 'partidos_jugados', 'victoria_total': 'victorias_total'})
    
    # --- FÓRMULAS DE ESTADÍSTICA AVANZADA (EQUIPOS) ---
    
    # Porcentaje de victorias: Qué parte de los partidos jugados ha ganado el equipo
    resultados_equipos['win_rate'] = (resultados_equipos['victorias_total'] / resultados_equipos['partidos_jugados']) * 100
    
    # True Shooting % del equipo: Eficiencia colectiva de tiro
    resultados_equipos['ts_porcentaje'] = (resultados_equipos['puntos_total'] / (2 * (resultados_equipos['tiros_campo_intentados_total'] + 0.44 * resultados_equipos['t1_intentados_total'] + 0.001))) * 100
    
    # Ratio de Asistencias: Porcentaje de jugadas que terminan en canasta tras una asistencia
    resultados_equipos['ast_ratio'] = (resultados_equipos['asistencias_total'] * 100) / (resultados_equipos['tiros_campo_intentados_total'] + 0.44 * resultados_equipos['t1_intentados_total'] + resultados_equipos['perdidas_total'] + 0.001)
    
    # Ratio de Rebote Ofensivo: Porcentaje de rebotes que el equipo captura en ataque sobre su total
    resultados_equipos['reb_ratio_ofensivo'] = (resultados_equipos['rebotes_ofensivos_total'] / (resultados_equipos['rebotes_totales_total'] + 0.001)) * 100
    
    # Posesiones Totales del Equipo: Estimación de cuántos ataques ha tenido el equipo en la temporada
    resultados_equipos['posesiones_totales'] = resultados_equipos['tiros_campo_intentados_total'] + 0.44 * resultados_equipos['t1_intentados_total'] - resultados_equipos['rebotes_ofensivos_total'] + resultados_equipos['perdidas_total']
    resultados_equipos['posesiones_por_partido'] = resultados_equipos['posesiones_totales'] / resultados_equipos['partidos_jugados'] # Ritmo de juego (Pace)
    
    # Rating Ofensivo del Equipo: Cuántos puntos mete el equipo cada 100 ataques
    resultados_equipos['ortg_equipo'] = (resultados_equipos['puntos_total'] / (resultados_equipos['posesiones_totales'] + 0.001)) * 100

    # Recuperamos el nombre oficial del equipo desde el maestro de equipos
    nombres_de_equipos = tabla_maestra_equipos[['uri_equipo', 'nombre_equipo']].drop_duplicates('uri_equipo')
    resultados_equipos_final = resultados_equipos.merge(nombres_de_equipos, on='uri_equipo', how='left')

    # Guardamos los resultados finales de los equipos con 2 decimales
    resultados_equipos_final.round(2).to_csv(os.path.join(CARPETA_CAPA3, 'capa3_equipos_avanzado.csv'), index=False)

    print(f"Proceso completado. Se han generado las estadisticas avanzadas para jugadores y equipos.") # Fin del proceso

if __name__ == "__main__": # Si se ejecuta el archivo directamente
    ejecutar_procesamiento_capa_3() # Lanzamos la analítica avanzada