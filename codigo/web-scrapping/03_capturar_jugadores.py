import time # Herramienta para manejar los tiempos y esperas
import os # Herramienta para manejar carpetas y archivos en tu ordenador
import re # Herramienta para buscar y filtrar textos complejos
import pandas as pd # Libreria principal para manejar tablas de datos
from io import StringIO # Herramienta para convertir texto en archivos virtuales
from bs4 import BeautifulSoup # Herramienta para analizar el codigo interno de las webs
from selenium import webdriver # Motor que permite controlar el navegador automaticamente
from selenium.webdriver.chrome.service import Service # Herramienta para gestionar el inicio de Google Chrome
from webdriver_manager.chrome import ChromeDriverManager # Herramienta que descarga el driver de Chrome por nosotros
from selenium.webdriver.chrome.options import Options # Herramienta para configurar como se abre el navegador
from selenium.webdriver.common.by import By # Herramienta para buscar elementos especificos dentro de una web
from selenium.webdriver.support.ui import WebDriverWait # Herramienta para hacer que el codigo espere a que cargue la web
from selenium.webdriver.support import expected_conditions as EC # Herramienta para definir que debe esperar el codigo

# --- CONFIGURACION DE RUTAS ---
# Guardamos la ruta donde esta tu archivo con la lista de todos los jugadores
RUTA_ARCHIVO_PLANTILLAS = os.path.join("datos", "bruto", "plantillas", "maestro_plantillas.csv")
# Guardamos la ruta de la carpeta donde se iran creando las temporadas y equipos
CARPETA_BASE_ESTADISTICAS = os.path.join("datos", "bruto", "temporadas")
# Definimos cada cuantos jugadores queremos que el navegador se cierre y se abra para no saturar el PC
CADA_CUANTO_REINICIAR_NAVEGADOR = 25 

def abrir_navegador(): # Funcion para poner en marcha el navegador
    configuracion = Options() # Creamos un objeto para guardar los ajustes del navegador
    configuracion.add_argument("--start-maximized") # Le decimos que se abra a pantalla completa
    configuracion.add_argument("--disable-blink-features=AutomationControlled") # Ajuste para que la web no nos detecte como un robot
    servicio_motor = Service(ChromeDriverManager().install()) # Instalamos y preparamos el motor de Chrome
    navegador_listo = webdriver.Chrome(service=servicio_motor, options=configuracion) # Arrancamos el navegador con nuestros ajustes
    navegador_listo.set_page_load_timeout(35) # Le damos un maximo de 35 segundos para cargar cada pagina
    return navegador_listo # Devolvemos el navegador listo para ser usado

def obtener_id_del_jugador(enlace_web): # Funcion para sacar el numero identificador del jugador de su URL
    try: # Intentamos hacer la operacion
        partes_de_la_url = str(enlace_web).split("/") # Dividimos la direccion web por cada barra inclinada que encontremos
        for trozo in partes_de_la_url: # Revisamos cada trozo de la direccion dividida
            if trozo.isdigit() and len(trozo) >= 3: # Si el trozo es un numero y tiene al menos 3 cifras
                return str(trozo) # Devolvemos ese numero como el ID del jugador
    except: pass # Si hay algun error, simplemente seguimos adelante
    return "ID_DESCONOCIDO" # Si no encontramos nada, le ponemos un nombre por defecto

def limpiar_nombre_para_archivo(texto_sucio): # Funcion para quitar caracteres raros que Windows no acepta en archivos
    if not texto_sucio: return "DESCONOCIDO" # Si no hay texto, devolvemos un nombre generico
    return str(texto_sucio).strip().replace(" ", "_").replace("/", "-").replace(".", "").replace('"', '').replace("'", "") # Quitamos espacios y simbolos

def formatear_anio_temporada(texto_web): # Funcion para convertir el formato 23-24 en el año 2023
    busqueda_patron = re.match(r'^(\d{2})-(\d{2})$', texto_web) # Buscamos si el texto tiene el formato de dos numeros, guion y dos numeros
    if busqueda_patron: # Si lo encontramos
        primer_numero = int(busqueda_patron.group(1)) # Cogemos el primer par de numeros
        return int(f"20{primer_numero:02d}") # Le añadimos el 20 delante para tener el año completo
    return None # Si el formato no coincide, devolvemos un valor vacio

def descargar_partidos_que_faltan(): # Funcion principal que coordina toda la descarga
    print("Iniciando revision de partidos pendientes (Solo Temporada Regular)...") # Mensaje informativo
    
    if not os.path.exists(RUTA_ARCHIVO_PLANTILLAS): # Si no encontramos el archivo de la lista de jugadores
        print("Error: No se encuentra el archivo maestro en la ruta especificada") # Avisamos del error
        return # Paramos la ejecucion

    # Leemos la lista de jugadores saltando lineas mal escritas y respetando las comillas
    lista_de_jugadores = pd.read_csv(RUTA_ARCHIVO_PLANTILLAS, on_bad_lines='skip', quotechar='"')
    
    cola_de_trabajo = {} # Creamos un diccionario para organizar que jugadores vamos a descargar
    conteo_ya_descargados = 0 # Contador para saber cuantos archivos ya tienes en el PC
    conteo_por_descargar = 0 # Contador para saber cuantos nos faltan

    print("Revisando que archivos tienes ya guardados en tu equipo...") # Mensaje informativo
    for _, fila in lista_de_jugadores.iterrows(): # Recorremos la lista de jugadores fila por fila
        enlace_jugador = fila['url_jugador'] # Cogemos la direccion web del jugador
        try: # Intentamos procesar la informacion de la fila
            anio_de_inicio = int(str(fila['temporada']).split("-")[0]) # Sacamos el año en el que empieza la temporada
            id_jugador = obtener_id_del_jugador(enlace_jugador) # Obtenemos su numero de ID
            nombre_de_la_temporada = str(fila['temporada']) # Guardamos el nombre de la temporada (ej: 2023-2024)
            nombre_equipo_limpio = limpiar_nombre_para_archivo(fila['nombre_equipo']) # Limpiamos el nombre del equipo
            nombre_jugador_limpio = limpiar_nombre_para_archivo(fila['nombre_jugador']) # Limpiamos el nombre del jugador
            
            # Definimos la carpeta y el nombre del archivo final
            carpeta_del_equipo = os.path.join(CARPETA_BASE_ESTADISTICAS, nombre_de_la_temporada, nombre_equipo_limpio)
            ruta_final_del_archivo = os.path.join(carpeta_del_equipo, f"{id_jugador}_{nombre_jugador_limpio}.csv")
            
            if os.path.exists(ruta_final_del_archivo) and os.path.getsize(ruta_final_del_archivo) > 50: # Si el archivo ya existe y tiene contenido
                conteo_ya_descargados += 1 # Lo sumamos a los que ya tenemos
            else: # Si no existe o esta vacio
                if enlace_jugador not in cola_de_trabajo: cola_de_trabajo[enlace_jugador] = [] # Preparamos la lista para este jugador
                cola_de_trabajo[enlace_jugador].append({
                    'anio': anio_de_inicio, 
                    'equipo': fila['nombre_equipo'], 
                    'ruta': ruta_final_del_archivo, 
                    'carpeta': carpeta_del_equipo
                }) # Añadimos la tarea de descarga
                conteo_por_descargar += 1 # Sumamos uno a los que faltan
        except: continue # Si algo falla en una fila, pasamos a la siguiente

    print(f"Archivos listos: {conteo_ya_descargados} | Archivos por bajar: {conteo_por_descargar}") # Resumen
    if conteo_por_descargar == 0: return # Si no falta nada por descargar, el programa termina aqui de forma segura

    # --- PARTE DE DESCARGA (Solo se ejecuta si faltan archivos) ---
    navegador = abrir_navegador() # Encendemos Google Chrome
    enlaces_a_visitar = list(cola_de_trabajo.keys()) # Sacamos la lista de todas las webs de jugadores pendientes
    
    for indice, enlace in enumerate(enlaces_a_visitar): # Empezamos a recorrer la lista de webs
        if indice > 0 and indice % CADA_CUANTO_REINICIAR_NAVEGADOR == 0: # Cada 25 jugadores
            navegador.quit() # Cerramos el navegador para que no se canse
            navegador = abrir_navegador() # Lo volvemos a abrir limpio
        
        print(f"Procesando perfil {indice + 1} de {len(enlaces_a_visitar)}: {enlace}") # Avisamos de por quien vamos

        try: # Intentamos entrar en el perfil del jugador
            navegador.get(enlace) # Cargamos la pagina del jugador
            time.sleep(1) # Esperamos un segundo
            codigo_perfil = BeautifulSoup(navegador.page_source, 'html.parser') # Analizamos el codigo de la pagina
            
            # Buscamos todos los años disponibles en el menu del jugador
            anios_en_la_web = {formatear_anio_temporada(a.text) for a in codigo_perfil.find_all('a') if formatear_anio_temporada(a.text)}
            
            for tarea in cola_de_trabajo[enlace]: # Para cada temporada que necesitemos de este jugador
                anio_buscado = tarea['anio'] # Definimos que año queremos bajar
                if anio_buscado in anios_en_la_web: # Si el año esta disponible en la web
                    direccion_estadisticas = f"{enlace}/partidos/{anio_buscado}" # Creamos la direccion de la tabla de partidos
                    navegador.get(direccion_estadisticas) # Vamos a esa pagina
                    try: # Intentamos capturar la tabla de la liga regular
                        WebDriverWait(navegador, 8).until(EC.presence_of_element_located((By.TAG_NAME, "table"))) # Esperamos a que salga la tabla
                        codigo_tabla = BeautifulSoup(navegador.page_source, 'html.parser') # Analizamos la pagina de estadisticas
                        
                        # Buscamos el titulo h2 o h3 que diga Temporada Regular
                        cabecera_regular = codigo_tabla.find(lambda etiqueta: etiqueta.name in ["h2", "h3"] and "Temporada Regular" in etiqueta.text)
                        
                        if cabecera_regular: # Si encontramos ese titulo
                            tabla_fase_regular = cabecera_regular.find_next("table") # Cogemos la tabla que esta justo debajo
                            datos_tabla = pd.read_html(StringIO(str(tabla_fase_regular)))[0] # Convertimos la tabla web en una tabla de Python
                            
                            os.makedirs(tarea['carpeta'], exist_ok=True) # Creamos la carpeta del equipo si no existe
                            datos_tabla.columns = [str(columna).upper() for columna in datos_tabla.columns] # Nombres de columna en mayusculas
                            if 'MIN' in datos_tabla.columns: # Si existe la columna de minutos
                                datos_tabla = datos_tabla[datos_tabla['MIN'] != 'MIN'] # Limpiamos filas de cabecera repetidas
                            
                            datos_tabla.to_csv(tarea['ruta'], index=False) # Guardamos el archivo en tu equipo
                            print(f"   Descargada Temporada Regular de {tarea['equipo']} ({anio_buscado})") # Exito
                        else: # Si no encontramos el titulo de Temporada Regular
                            print(f"   Saltando {anio_buscado}: Solo hay datos de Playoffs o Copa") # Aviso
                            os.makedirs(tarea['carpeta'], exist_ok=True) # Creamos la carpeta de todos modos
                            with open(tarea['ruta'], 'w') as archivo_vacio: archivo_vacio.write("PARTIDO,FECHA\nSALTADO,PLAYOFFS") # Dejamos una marca

                    except Exception as error_tabla: # Si falla la lectura de la tabla
                        print(f"   Error al intentar leer la tabla del año {anio_buscado}: {error_tabla}")
                else: # Si el año no figura en la web
                    print(f"   El año {anio_buscado} no figura en la web de este jugador.")
        except Exception as error_perfil: # Si falla la entrada al perfil
            print(f"Error general al entrar en el perfil: {error_perfil}")
            navegador.quit() # Cerramos navegador
            navegador = abrir_navegador() # Reiniciamos para el siguiente

    navegador.quit() # Cerramos el navegador al terminar todo
    print("Sincronizacion terminada con éxito.") # Despedida

if __name__ == "__main__": # Si ejecutas el archivo
    descargar_partidos_que_faltan() # Arrancamos el proceso