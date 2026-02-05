import time # Herramienta para manejar los tiempos de espera
import os # Herramienta para gestionar carpetas y archivos en tu ordenador
import pandas as pd # Libreria principal para trabajar con tablas de datos
from bs4 import BeautifulSoup # Herramienta para leer y analizar el codigo de las paginas web
from selenium import webdriver # Motor para controlar el navegador de forma automatica
from selenium.webdriver.chrome.service import Service # Herramienta para iniciar el servicio de Chrome
from webdriver_manager.chrome import ChromeDriverManager # Descargador automatico del driver de Chrome
from selenium.webdriver.chrome.options import Options # Configuracion para el comportamiento del navegador
from selenium.webdriver.common.by import By # Herramienta para buscar elementos en la web
from selenium.webdriver.support.ui import WebDriverWait # Herramienta para que el codigo sepa esperar
from selenium.webdriver.support import expected_conditions as EC # Condiciones que el codigo debe esperar

# --- CONFIGURACION DE RUTAS ---
# Ruta donde esta guardado el archivo con los equipos y sus enlaces
RUTA_MAESTRO_EQUIPOS = os.path.join("datos", "bruto", "equipos", "maestro_equipos.csv")
# Ruta donde se guardara la lista final de jugadores por equipo
RUTA_MAESTRO_PLANTILLAS = os.path.join("datos", "bruto", "plantillas", "maestro_plantillas.csv")
# Numero de equipos procesados antes de reiniciar el navegador para que no vaya lento
REINICIAR_CADA_X_EQUIPOS = 15 

def iniciar_el_navegador(): # Funcion para arrancar el navegador Chrome
    ajustes_navegador = Options() # Creamos un paquete de ajustes para el navegador
    ajustes_navegador.add_argument("--start-maximized") # Le decimos que se abra con la ventana grande
    ajustes_navegador.add_argument("--disable-blink-features=AutomationControlled") # Evitamos que la web nos bloquee por ser un robot
    servicio_motor = Service(ChromeDriverManager().install()) # Preparamos el motor de Chrome mas actual
    navegador_abierto = webdriver.Chrome(service=servicio_motor, options=ajustes_navegador) # Encendemos el navegador
    navegador_abierto.set_page_load_timeout(40) # Le damos 40 segundos maximo para cargar cada pagina
    return navegador_abierto # Devolvemos el navegador listo para trabajar

def extraer_lista_de_plantillas(): # Funcion principal para sacar los jugadores de cada equipo
    print("Iniciando extraccion de plantillas (Filtrando solo Temporada Regular)...") # Mensaje de inicio
    
    if not os.path.exists(RUTA_MAESTRO_EQUIPOS): # Comprobamos si el archivo de equipos existe
        print("Error: No se encuentra el archivo de equipos en la ruta") # Si no existe, avisamos
        return # Y paramos el programa

    tabla_equipos = pd.read_csv(RUTA_MAESTRO_EQUIPOS) # Leemos la tabla de equipos con los enlaces
    
    equipos_ya_listos = set() # Creamos una lista para recordar que equipos ya hemos hecho
    if os.path.exists(RUTA_MAESTRO_PLANTILLAS): # Si ya existe el archivo de plantillas de antes
        try: # Intentamos leerlo
            tabla_anterior = pd.read_csv(RUTA_MAESTRO_PLANTILLAS, on_bad_lines='skip') # Cargamos lo que ya tenemos
            for _, fila_datos in tabla_anterior.iterrows(): # Miramos fila por fila
                equipos_ya_listos.add(f"{fila_datos['nombre_equipo']}_{fila_datos['temporada']}") # Guardamos el nombre y año
            print(f"Se han detectado {len(equipos_ya_listos)} equipos ya procesados. Saltando...") # Informamos
        except: pass # Si hay algun fallo leyendo, seguimos adelante

    navegador = iniciar_el_navegador() # Arrancamos el navegador
    bolsa_de_jugadores = [] # Creamos un saco donde guardar los nuevos jugadores encontrados
    contador_de_equipos = 0 # Iniciamos un contador para saber por que numero de equipo vamos

    try: # Iniciamos el proceso principal
        for numero_fila, datos_fila in tabla_equipos.iterrows(): # Recorremos la lista de equipos uno a uno
            nombre_equipo_actual = datos_fila['nombre_equipo'] # Sacamos el nombre del equipo
            temporada_actual = datos_fila['temporada'] # Sacamos la temporada
            enlace_equipo = datos_fila['url_equipo'] # Sacamos el enlace de su web
            
            if f"{nombre_equipo_actual}_{temporada_actual}" in equipos_ya_listos: # Si ya lo habiamos procesado antes
                continue # Saltamos al siguiente de la lista

            if contador_de_equipos > 0 and contador_de_equipos % REINICIAR_CADA_X_EQUIPOS == 0: # Si toca reiniciar
                print("Reiniciando navegador para mantener la velocidad...") # Avisamos
                navegador.quit() # Cerramos el actual
                navegador = iniciar_el_navegador() # Abrimos uno nuevo limpio

            print(f"Procesando equipo {numero_fila + 1}: {nombre_equipo_actual} ({temporada_actual})") # Informamos
            
            try: # Intentamos entrar en la web del equipo
                navegador.get(enlace_equipo) # Vamos a la direccion web del equipo
                WebDriverWait(navegador, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table"))) # Esperamos a que salga la tabla
                time.sleep(2) # Esperamos 2 segundos extra para que cargue todo bien

                codigo_html = BeautifulSoup(navegador.page_source, 'html.parser') # Analizamos el codigo de la pagina
                
                # Buscamos el titulo exacto que dice Temporada Regular
                titulo_regular = codigo_html.find(lambda etiqueta: etiqueta.name in ["h2", "h3"] and "Temporada Regular" in etiqueta.text)
                
                if titulo_regular: # Si encontramos ese titulo
                    tabla_seleccionada = titulo_regular.find_next("table") # Cogemos la tabla que esta justo debajo
                    filas_jugadores = tabla_seleccionada.find_all('tr') # Sacamos todas las filas de esa tabla
                    print("   Tabla de Temporada Regular localizada con éxito.") # Confirmamos
                else: # Si la web no tiene titulos separados
                    filas_jugadores = codigo_html.find_all('tr') # Cogemos todas las filas que haya en la pagina
                    print("   No se encontro division de secciones. Usando tablas generales.") # Avisamos

                jugadores_nuevos_conteo = 0 # Contador de jugadores para este equipo
                direcciones_procesadas = set() # Evitamos guardar al mismo jugador dos veces en el mismo equipo
                
                for fila_tabla in filas_jugadores: # Revisamos cada fila de la tabla encontrada
                    enlace_perfil = fila_tabla.find('a', href=True) # Buscamos si hay un enlace
                    if enlace_perfil and ('/jugador/' in enlace_perfil['href'] or '/player/' in enlace_perfil['href']): # Si es un enlace a un jugador
                        columnas_datos = fila_tabla.find_all(['td', 'th']) # Miramos cuantas columnas tiene la fila
                        if len(columnas_datos) >= 2: # Si tiene informacion suficiente
                            enlace_completo = enlace_perfil['href'] if enlace_perfil['href'].startswith("http") else f"https://www.proballers.com{enlace_perfil['href']}" # Completamos la web
                            
                            if enlace_completo not in direcciones_procesadas: # Si no lo habiamos anotado ya
                                bolsa_de_jugadores.append({ # Guardamos los datos del jugador en el saco
                                    "temporada": temporada_actual,
                                    "nombre_equipo": nombre_equipo_actual,
                                    "nombre_jugador": enlace_perfil.get_text(strip=True),
                                    "url_jugador": enlace_completo,
                                    "uri_equipo": enlace_equipo
                                })
                                direcciones_procesadas.add(enlace_completo) # Lo marcamos como anotado
                                jugadores_nuevos_conteo += 1 # Sumamos uno al contador
                
                print(f"   Se han extraido {jugadores_nuevos_conteo} jugadores correctamente.") # Resumen del equipo
                
                if (contador_de_equipos + 1) % 5 == 0 and bolsa_de_jugadores: # Cada 5 equipos guardamos en el archivo
                    modo_archivo = 'a' if os.path.exists(RUTA_MAESTRO_PLANTILLAS) else 'w' # Decidimos si añadir o crear
                    pd.DataFrame(bolsa_de_jugadores).to_csv(RUTA_MAESTRO_PLANTILLAS, mode=modo_archivo, header=(modo_archivo=='w'), index=False) # Guardamos
                    bolsa_de_jugadores = [] # Vaciamos el saco para los siguientes equipos
                        
            except Exception as error_detalle: # Si falla un equipo concreto
                print(f"Error procesando este equipo: {error_detalle}") # Avisamos del error
                navegador.quit() # Cerramos navegador
                navegador = iniciar_el_navegador() # Lo abrimos de nuevo para seguir con el resto
            
            contador_de_equipos += 1 # Sumamos uno al contador de sesion

    finally: # Al terminar todo el proceso
        navegador.quit() # Cerramos el navegador definitivamente
        if bolsa_de_jugadores: # Si quedaban jugadores en el saco sin guardar
            modo_archivo = 'a' if os.path.exists(RUTA_MAESTRO_PLANTILLAS) else 'w' # Decidimos modo
            pd.DataFrame(bolsa_de_jugadores).to_csv(RUTA_MAESTRO_PLANTILLAS, mode=modo_archivo, header=(modo_archivo=='w'), index=False) # Guardamos lo ultimo
            print("Guardado final de datos completado.") # Mensaje de despedida

if __name__ == "__main__": # Si ejecutamos este archivo directamente
    extraer_lista_de_plantillas() # Arrancamos el motor de extraccion