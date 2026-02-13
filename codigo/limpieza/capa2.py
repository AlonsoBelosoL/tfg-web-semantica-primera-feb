import pandas as pd # Importamos la librería pandas para el manejo de tablas de datos
import os # Importamos os para navegar por las carpetas del sistema
import re # Importamos re para realizar búsquedas de texto con expresiones regulares
import unicodedata # Importamos unicodedata para quitar tildes y normalizar caracteres
from collections import Counter # Importamos Counter para contar elementos de forma eficiente
from difflib import SequenceMatcher # Importamos SequenceMatcher para comparar la similitud entre nombres

# --- 1. CONFIGURACIÓN Y DICCIONARIOS DE APOYO ---

# Diccionario para convertir abreviaturas de meses a números, incluyendo el caso especial de septiembre
DICCIONARIO_MESES = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sep': '09', 'sept': '09', 
    'oct': '10', 'nov': '11', 'dic': '12'
}

# Diccionario con direcciones web fijas para corregir errores conocidos de nombres de carpetas
CORRECCIONES_EQUIPOS_MANUALES = {
    'ii': 'https://www.proballers.com/es/baloncesto/equipo/2244/fc-barcelona-ii',
    'rvb': 'https://www.proballers.com/es/baloncesto/equipo/146/real-valladolid',
    'manresa': 'https://www.proballers.com/es/baloncesto/equipo/214/manresa',
    'ourence': 'https://www.proballers.com/es/baloncesto/equipo/670/ourense',
    'leyma coruna': 'https://www.proballers.com/es/baloncesto/equipo/2114/leyma-coruna'
}

# --- 2. FUNCIONES DE LIMPIEZA Y BÚSQUEDA ---

def normalizar_texto_equipo(texto): # Función para limpiar nombres y que sean comparables
    if pd.isna(texto): return "" # Si el texto está vacío, devolvemos un texto en blanco
    texto_limpio = unicodedata.normalize('NFD', str(texto).lower()) # Pasamos a minúsculas y separamos tildes
    texto_limpio = "".join([caracter for caracter in texto_limpio if unicodedata.category(caracter) != 'Mn']) # Quitamos las tildes
    texto_limpio = re.sub(r'[^\w\s-]', '', texto_limpio).replace('-', ' ') # Quitamos símbolos y dejamos espacios
    palabras_ruido = ['club', 'baloncesto', 'sad', 'cb', 'basket', 'basketball', 'monbus', 'movistar', 'leyma', '1', 'rio', 'sur', 'aspasia'] # Palabras que no aportan significado
    for palabra in palabras_ruido: texto_limpio = f" {texto_limpio} ".replace(f" {palabra} ", " ") # Borramos las palabras de ruido
    return texto_limpio.strip() # Devolvemos el texto sin espacios sobrantes en los extremos

def encontrar_direccion_equipo(nombre_buscar, temporada, lista_maestra, direccion_excluir=None): # Busca la dirección web de un equipo
    nombre_busqueda = normalizar_texto_equipo(nombre_buscar) # Limpiamos el nombre que queremos buscar
    if nombre_busqueda in CORRECCIONES_EQUIPOS_MANUALES: return CORRECCIONES_EQUIPOS_MANUALES[nombre_busqueda] # Si está en las correcciones manuales, lo devolvemos
    mejor_direccion, puntuacion_maxima = "equipo_desconocido", 0.0 # Preparamos variables para guardar el mejor resultado
    for equipo in lista_maestra: # Recorremos la lista de equipos del archivo maestro
        if equipo['temporada'] == temporada: # Solo comparamos equipos de la misma temporada
            if direccion_excluir and equipo['direccion_estable'] == direccion_excluir: continue # Evitamos que un equipo juegue contra sí mismo
            puntuacion = max(SequenceMatcher(None, nombre_busqueda, equipo['nombre_limpio']).ratio(), # Calculamos similitud con el nombre
                        SequenceMatcher(None, nombre_busqueda, equipo['identificador_url']).ratio()) # Calculamos similitud con la dirección web
            if puntuacion > puntuacion_maxima and puntuacion >= 0.60: # Si la similitud es alta y es la mejor hasta ahora
                puntuacion_maxima, mejor_direccion = puntuacion, equipo['direccion_estable'] # Guardamos la puntuación y la dirección
    return mejor_direccion # Devolvemos la dirección encontrada o el texto de desconocido

def separar_intentos_tiros(valor): # Divide textos como "5-8" en aciertos e intentos
    if pd.isna(valor) or '-' not in str(valor): return 0, 0 # Si el dato no es válido, devolvemos ceros
    partes = str(valor).split('-') # Cortamos el texto por el guion
    try: return int(partes[0]), int(partes[1]) # Intentamos devolver los dos números como enteros
    except: return 0, 0 # Si falla, devolvemos ceros

def limpiar_valor_numerico(valor): # Convierte textos con comas o guiones en números reales
    if pd.isna(valor) or valor == '-' or valor == '': return 0.0 # Si no hay dato, devolvemos cero
    try: return float(str(valor).replace(',', '.')) # Cambiamos comas por puntos y pasamos a decimal
    except: return 0.0 # Si falla la conversión, devolvemos cero

# --- 3. PROCESAMIENTO PRINCIPAL ---

def procesar_capa_2_completa(): # Función que coordina toda la limpieza e integración
    print("Iniciando Capa 2: Motor de Integridad Total (Deduplicado y Logica de Marcadores)...") # Mensaje de inicio
    
    tabla_equipos_capa1 = pd.read_csv('datos/procesados/capa1/capa1_equipos_temporada.csv') # Cargamos los equipos de la capa 1
    tabla_jugadores_capa1 = pd.read_csv('datos/procesados/capa1/capa1_jugadores.csv', on_bad_lines='skip') # Cargamos los jugadores de la capa 1
    
    lista_referencia_maestra = [{'temporada': fila['temporada'], 'direccion_estable': fila['uri_equipo'], 
                    'nombre_limpio': normalizar_texto_equipo(fila['nombre_equipo']), 
                    'identificador_url': normalizar_texto_equipo(fila['uri_equipo'].split('/')[-1])} for _, fila in tabla_equipos_capa1.iterrows()] # Preparamos una lista rápida de equipos
    
    diccionario_mapeo_jugadores = {} # Diccionario para encontrar la web del jugador por su número de ID
    for _, fila in tabla_jugadores_capa1.iterrows(): # Recorremos los jugadores del maestro
        busqueda_id = re.search(r'/jugador/(\d+)/', str(fila['url_jugador'])) # Buscamos el número identificador en su dirección web
        if busqueda_id: diccionario_mapeo_jugadores[busqueda_id.group(1)] = fila['url_jugador'] # Si lo encontramos, lo guardamos en el diccionario

    ruta_carpetas_temporada = 'datos/bruto/temporadas/' # Definimos donde están las carpetas de los años
    diccionario_partidos_unificados = {} # Para guardar la información general de cada partido
    diccionario_estadisticas_detalladas = {} # Para guardar las estadísticas de cada jugador en cada partido

    for nombre_temporada in sorted(os.listdir(ruta_carpetas_temporada)): # Recorremos cada carpeta de temporada
        ruta_temporada = os.path.join(ruta_carpetas_temporada, nombre_temporada) # Construimos la ruta completa de la temporada
        if not os.path.isdir(ruta_temporada): continue # Si no es una carpeta, la saltamos
        año_inicio_temporada = int(nombre_temporada.split('-')[0]) # Sacamos el año en que empieza la temporada

        for nombre_carpeta_equipo in os.listdir(ruta_temporada): # Recorremos las carpetas de los equipos
            direccion_equipo_carpeta = encontrar_direccion_equipo(nombre_carpeta_equipo.replace('_', ' '), nombre_temporada, lista_referencia_maestra) # Buscamos la dirección web oficial del equipo
            if direccion_equipo_carpeta == "equipo_desconocido": continue # Si no sabemos qué equipo es, lo saltamos
            
            ruta_equipo = os.path.join(ruta_temporada, nombre_carpeta_equipo) # Construimos la ruta de la carpeta del equipo
            for nombre_archivo_jugador in os.listdir(ruta_equipo): # Recorremos los archivos de cada jugador
                identificador_jugador_texto = nombre_archivo_jugador.split('_')[0] # Sacamos el número de ID del nombre del archivo
                direccion_web_jugador = diccionario_mapeo_jugadores.get(identificador_jugador_texto, f"desconocido_{identificador_jugador_texto}") # Obtenemos su dirección web oficial
                try: # Intentamos abrir el archivo del jugador
                    tabla_estadisticas_jugador = pd.read_csv(os.path.join(ruta_equipo, nombre_archivo_jugador)).iloc[::-1].reset_index(drop=True) # Lo leemos y le damos la vuelta para que sea cronológico
                except: continue # Si el archivo está roto, pasamos al siguiente

                for indice_fila, datos_fila in tabla_estadisticas_jugador.iterrows(): # Recorremos los partidos del jugador
                    partes_fecha = str(datos_fila['FECHA']).lower().split() # Dividimos la fecha en palabras
                    if len(partes_fecha) < 3: continue # Si la fecha está mal escrita, la saltamos
                    mes_numero = DICCIONARIO_MESES.get(partes_fecha[1].replace('.', ''), '01') # Convertimos el mes a número (soporta 'sept')
                    fecha_estandar = f"{partes_fecha[2]}-{mes_numero}-{partes_fecha[0].zfill(2)}" # Creamos la fecha en formato año-mes-día
                    
                    jugador_es_visitante = '@' in datos_fila['PARTIDO'] # Miramos si el jugador jugaba fuera de casa
                    nombre_equipo_rival = datos_fila['PARTIDO'].replace('vs ', '').replace('@ ', '').strip() # Limpiamos el nombre del rival
                    direccion_equipo_rival = encontrar_direccion_equipo(nombre_equipo_rival, nombre_temporada, lista_referencia_maestra, direccion_excluir=direccion_equipo_carpeta) # Buscamos la dirección del rival
                    if direccion_equipo_rival == "equipo_desconocido": continue # Si el rival es desconocido, saltamos el partido

                    direccion_local = direccion_equipo_rival if jugador_es_visitante else direccion_equipo_carpeta # Definimos quién es el equipo local
                    direccion_visitante = direccion_equipo_carpeta if jugador_es_visitante else direccion_equipo_rival # Definimos quién es el equipo visitante

                    slugs_ordenados = sorted([direccion_local.split('/')[-1], direccion_visitante.split('/')[-1]]) # Ordenamos los nombres de los equipos alfabéticamente
                    identificador_unico_partido = f"{fecha_estandar.replace('-','')}_{slugs_ordenados[0]}_{slugs_ordenados[1]}" # Creamos un ID único para el partido

                    if identificador_unico_partido not in diccionario_partidos_unificados: # Si es la primera vez que vemos este partido
                        busqueda_puntuacion = re.search(r'(\d+)-(\d+)', str(datos_fila['PUNTUACIÓN'])) # Buscamos los números del marcador
                        if busqueda_puntuacion: # Si encontramos el marcador
                            puntos_uno, puntos_dos = int(busqueda_puntuacion.group(1)), int(busqueda_puntuacion.group(2)) # Sacamos los dos números
                            letra_resultado = str(datos_fila['PUNTUACIÓN']).split()[0] # Miramos si pone G (ganó) o P (perdió)
                            puntos_mi_equipo = max(puntos_uno, puntos_dos) if 'G' in letra_resultado else min(puntos_uno, puntos_dos) # Asignamos los puntos del equipo del jugador
                            puntos_rival = min(puntos_uno, puntos_dos) if 'G' in letra_resultado else max(puntos_uno, puntos_dos) # Asignamos los puntos del rival
                            
                            puntos_local = puntos_mi_equipo if not jugador_es_visitante else puntos_rival # Guardamos los puntos del local
                            puntos_visitante = puntos_rival if not jugador_es_visitante else puntos_mi_equipo # Guardamos los puntos del visitante
                            
                            diccionario_partidos_unificados[identificador_unico_partido] = { # Guardamos los datos generales del partido
                                'id_partido': identificador_unico_partido, 'fecha': fecha_estandar, 'temporada': nombre_temporada, 'ano_inicio': año_inicio_temporada,
                                'jornada': int(indice_fila + 1), # <-- MODIFICACIÓN: Añadida jornada al archivo de partidos
                                'uri_local': direccion_local, 'uri_visitante': direccion_visitante,
                                'puntos_local': puntos_local, 'puntos_visitante': puntos_visitante
                            }

                    if (identificador_unico_partido, direccion_web_jugador) not in diccionario_estadisticas_detalladas: # Evitamos duplicar al mismo jugador en el mismo partido
                        t2_m, t2_i = separar_intentos_tiros(datos_fila.get('2M-2A')) # Procesamos tiros de dos
                        t3_m, t3_i = separar_intentos_tiros(datos_fila.get('3M-3A')) # Procesamos tiros de tres
                        t1_m, t1_i = separar_intentos_tiros(datos_fila.get('1M-1A')) # Procesamos tiros libres
                        diccionario_estadisticas_detalladas[(identificador_unico_partido, direccion_web_jugador)] = { # Guardamos las estadísticas del jugador
                            'url_jugador': direccion_web_jugador, 'id_partido': identificador_unico_partido, 'uri_equipo': direccion_equipo_carpeta,
                            'uri_rival': direccion_equipo_rival, 'ano_inicio': año_inicio_temporada, 'jornada': int(indice_fila + 1),
                            'minutos': str(datos_fila.get('MIN', '0')), 'puntos': limpiar_valor_numerico(datos_fila.get('PTS')),
                            'valoracion': limpiar_valor_numerico(datos_fila.get('VAL')), 't2_metidos': t2_m, 't2_intentados': t2_i,
                            't3_metidos': t3_m, 't3_intentados': t3_i, 't1_metidos': t1_m, 't1_intentados': t1_i,
                            'rebotes_ofensivos': limpiar_valor_numerico(datos_fila.get('RO')), 'rebotes_defensivos': limpiar_valor_numerico(datos_fila.get('RD')),
                            'rebotes_totales': limpiar_valor_numerico(datos_fila.get('REB.1', datos_fila.get('REB', 0))),
                            'asistencias': limpiar_valor_numerico(datos_fila.get('AST.1', datos_fila.get('AST', 0))),
                            'robos': limpiar_valor_numerico(datos_fila.get('BR')), 'tapones': limpiar_valor_numerico(datos_fila.get('TAP')),
                            'perdidas': limpiar_valor_numerico(datos_fila.get('BP')), 'mas_menos': limpiar_valor_numerico(datos_fila.get('+/-')),
                            'faltas_cometidas': limpiar_valor_numerico(datos_fila.get('FC', datos_fila.get('F', 0))),
                            'faltas_recibidas': limpiar_valor_numerico(datos_fila.get('FR'))
                        }

    tabla_estadisticas_final = pd.DataFrame(list(diccionario_estadisticas_detalladas.values())) # Convertimos todas las estadísticas a una tabla
    identificadores_validos = tabla_estadisticas_final.groupby('id_partido').size()[tabla_estadisticas_final.groupby('id_partido').size() >= 5].index # Buscamos partidos con al menos 5 jugadores
    tabla_partidos_limpia = pd.DataFrame([valor for clave, valor in diccionario_partidos_unificados.items() if clave in identificadores_validos]) # Nos quedamos solo con esos partidos generales
    tabla_estadisticas_limpia = tabla_estadisticas_final[tabla_estadisticas_final['id_partido'].isin(identificadores_validos)] # Nos quedamos solo con las estadísticas de esos partidos

    print("Fase 3: Agregando estadisticas y calculando coberturas...") # Mensaje de progreso
    for indice_partido, fila_partido in tabla_partidos_limpia.iterrows(): # Recorremos los partidos validados
        for equipo_rol in ['local', 'visitante']: # Hacemos el cálculo para el local y luego para el visitante
            direccion_web_equipo = fila_partido[f'uri_{equipo_rol}'] # Obtenemos la dirección del equipo
            registros_equipo = tabla_estadisticas_limpia[(tabla_estadisticas_limpia['id_partido'] == fila_partido['id_partido']) & (tabla_estadisticas_limpia['uri_equipo'] == direccion_web_equipo)] # Filtramos los jugadores de ese equipo en ese partido
            if not registros_equipo.empty: # Si hemos encontrado jugadores
                puntos_oficiales = fila_partido[f'puntos_{equipo_rol}'] # Cogemos los puntos que dice el marcador
                suma_puntos_jugadores = registros_equipo['puntos'].sum() # Sumamos los puntos que dicen los jugadores
                tabla_partidos_limpia.at[indice_partido, f'cobertura_{equipo_rol}'] = round(suma_puntos_jugadores / puntos_oficiales, 2) if puntos_oficiales > 0 else 0 # Calculamos cuánto cubren los jugadores sobre el total
                tabla_partidos_limpia.at[indice_partido, f'rebotes_{equipo_rol}'] = int(registros_equipo['rebotes_totales'].sum()) # Sumamos rebotes totales
                tabla_partidos_limpia.at[indice_partido, f'asistencias_{equipo_rol}'] = int(registros_equipo['asistencias'].sum()) # Sumamos asistencias totales
                tabla_partidos_limpia.at[indice_partido, f'robos_{equipo_rol}'] = int(registros_equipo['robos'].sum()) # Sumamos robos totales
                tabla_partidos_limpia.at[indice_partido, f'perdidas_{equipo_rol}'] = int(registros_equipo['perdidas'].sum()) # Sumamos pérdidas totales
                tabla_partidos_limpia.at[indice_partido, f'valoracion_{equipo_rol}'] = int(registros_equipo['valoracion'].sum()) # Sumamos valoración total
                intentos_tres = registros_equipo['t3_intentados'].sum() # Sumamos intentos de tres puntos
                tabla_partidos_limpia.at[indice_partido, f'porc_t3_{equipo_rol}'] = round((registros_equipo['t3_metidos'].sum() / (intentos_tres + 0.001)) * 100, 2) # Calculamos el porcentaje de acierto en triples

    os.makedirs('datos/procesados/capa2', exist_ok=True) # Creamos la carpeta de destino si no existe
    tabla_partidos_limpia.to_csv('datos/procesados/capa2/capa2_partidos.csv', index=False) # Guardamos el archivo de partidos
    tabla_estadisticas_limpia.to_csv('datos/procesados/capa2/capa2_estadisticas_detalladas.csv', index=False) # Guardamos el archivo detallado de jugadores
    print(f"Proceso finalizado. Partidos: {len(tabla_partidos_limpia)}. Registros detallados: {len(tabla_estadisticas_limpia)}") # Mensaje de despedida con resumen

if __name__ == "__main__": # Punto de entrada del script
    procesar_capa_2_completa() # Llamamos a la función principal