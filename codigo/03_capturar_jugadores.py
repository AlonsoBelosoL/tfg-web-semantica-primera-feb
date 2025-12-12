import time
import os
import re
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- CONFIGURACIÓN GLOBAL ---
RUTA_MAESTRO_PLANTILLAS = os.path.join("datos", "bruto", "plantillas", "maestro_plantillas.csv")
CARPETA_RAIZ_ESTADISTICAS = os.path.join("datos", "bruto", "temporadas")
FRECUENCIA_REINICIO_DRIVER = 30

def iniciar_navegador():
    opciones = Options()
    opciones.add_argument("--start-maximized")
    # opciones.add_argument("--headless")
    servicio = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=servicio, options=opciones)
    driver.set_page_load_timeout(30)
    return driver

def extraer_id_jugador(url):
    """Extrae el identificador numérico de la URL del jugador."""
    try:
        partes = str(url).split("/")
        for parte in partes:
            if parte.isdigit() and len(parte) >= 3: 
                return str(parte)
    except:
        pass
    return "ID_DESCONOCIDO"

def limpiar_nombre_archivo(nombre):
    """Sanitiza el string para que sea válido como nombre de archivo en el sistema operativo."""
    return str(nombre).strip().replace(" ", "_").replace("/", "-").replace(".", "").replace('"', '').replace("'", "")

def normalizar_temporada(texto):
    """
    Convierte formatos de temporada cortos (15-16) a formato de año inicial (2015).
    """
    coincidencia = re.match(r'^(\d{2})-(\d{2})$', texto)
    if coincidencia:
        n1 = int(coincidencia.group(1))
        n2 = int(coincidencia.group(2))
        if (n2 == (n1 + 1) % 100):
            return int(f"19{n1:02d}") if n1 > 50 else int(f"20{n1:02d}")
    return None

def descargar_estadisticas_faltantes():
    """
    Audita los archivos existentes y descarga las estadísticas de juego (Game Logs)
    para los jugadores faltantes en la estructura de carpetas.
    """
    print("Iniciando sincronización de estadísticas de jugadores...")
    
    if not os.path.exists(RUTA_MAESTRO_PLANTILLAS):
        print(f"Error: No se encuentra el archivo maestro {RUTA_MAESTRO_PLANTILLAS}")
        return

    df_plantillas = pd.read_csv(RUTA_MAESTRO_PLANTILLAS)
    print(f"Registros totales en maestro: {len(df_plantillas)}")

    # 1. Fase de Auditoría
    # Agrupamos tareas por URL de jugador para optimizar la navegación
    tareas_descarga = {} 
    
    archivos_existentes = 0
    archivos_pendientes = 0

    print("Auditando archivos locales...")
    
    for _, fila in df_plantillas.iterrows():
        url_jugador = fila['url_jugador']
        nombre_jugador = fila['nombre_jugador']
        nombre_equipo = fila['nombre_equipo']
        temporada_str = str(fila['temporada']) # Formato esperado: "2015-2016"
        
        try:
            anio_inicio = int(temporada_str.split("-")[0])
            id_jugador = extraer_id_jugador(url_jugador)
            
            # Definición de rutas
            nombre_carpeta_temp = f"{anio_inicio}-{anio_inicio+1}"
            carpeta_destino = os.path.join(CARPETA_RAIZ_ESTADISTICAS, nombre_carpeta_temp, limpiar_nombre_archivo(nombre_equipo))
            nombre_fichero = f"{id_jugador}_{limpiar_nombre_archivo(nombre_jugador)}.csv"
            ruta_absoluta = os.path.join(carpeta_destino, nombre_fichero)
            
            # Verificación de existencia y validez (tamaño > 10 bytes)
            if os.path.exists(ruta_absoluta) and os.path.getsize(ruta_absoluta) > 10:
                archivos_existentes += 1
            else:
                # Añadir a la cola de descarga
                if url_jugador not in tareas_descarga:
                    tareas_descarga[url_jugador] = []
                
                tareas_descarga[url_jugador].append({
                    'anio': anio_inicio,
                    'equipo': nombre_equipo,
                    'ruta_destino': ruta_absoluta,
                    'carpeta_destino': carpeta_destino
                })
                archivos_pendientes += 1
                
        except Exception as e:
            print(f"Advertencia procesando fila CSV: {e}")

    print(f"Archivos verificados: {archivos_existentes}")
    print(f"Archivos pendientes de descarga: {archivos_pendientes}")
    
    if archivos_pendientes == 0:
        print("Sincronización completa. No hay descargas pendientes.")
        return

    # 2. Fase de Descarga
    driver = iniciar_navegador()
    contador_sesion = 0
    urls_unicas = list(tareas_descarga.keys())
    
    for i, url in enumerate(urls_unicas):
        lista_tareas = tareas_descarga[url]
        
        # Mantenimiento del driver
        if contador_sesion > 0 and contador_sesion % FRECUENCIA_REINICIO_DRIVER == 0:
            print("Refrescando navegador...")
            driver.quit()
            time.sleep(2)
            driver = iniciar_navegador()
        
        print(f"\n[{i+1}/{len(urls_unicas)}] Visitando perfil: {url}")
        print(f" -> Temporadas requeridas: {len(lista_tareas)}")

        try:
            # A) Navegar al perfil principal
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # B) Identificar años disponibles en la navegación del sitio
            enlaces_web = soup.find_all('a', href=True)
            anios_disponibles_web = set()
            
            for link in enlaces_web:
                texto_link = link.get_text(strip=True)
                # Filtrar enlaces irrelevantes
                if '/partido/' not in link['href'] and '/game/' not in link['href']:
                    anio_detectado = normalizar_temporada(texto_link)
                    if anio_detectado:
                        anios_disponibles_web.add(anio_detectado)
            
            # C) Procesar descargas coincidentes
            for tarea in lista_tareas:
                anio_objetivo = tarea['anio']
                ruta_final = tarea['ruta_destino']
                carpeta_final = tarea['carpeta_destino']
                
                if anio_objetivo in anios_disponibles_web:
                    url_estadisticas = f"{url}/partidos/{anio_objetivo}"
                    
                    try:
                        driver.get(url_estadisticas)
                        html_tabla = driver.page_source
                        
                        # Lectura de tablas HTML con Pandas
                        tablas_encontradas = pd.read_html(StringIO(html_tabla))
                        df_resultado = None
                        
                        # Buscar la tabla correcta (debe contener MIN o PTS)
                        if tablas_encontradas:
                            for tabla in tablas_encontradas:
                                cols_upper = [str(c).upper() for c in tabla.columns]
                                if 'MIN' in cols_upper or 'PTS' in cols_upper:
                                    df_resultado = tabla
                                    break
                        
                        if df_resultado is not None:
                            os.makedirs(carpeta_final, exist_ok=True)
                            
                            # Normalización de columnas
                            df_resultado.columns = [str(c).upper() for c in df_resultado.columns]
                            
                            # Filtrado de filas de cabecera repetidas
                            if 'MIN' in df_resultado.columns:
                                df_resultado = df_resultado[df_resultado['MIN'] != 'MIN']
                            
                            df_resultado.to_csv(ruta_final, index=False)
                            print(f"    -> Descargado: {tarea['equipo']} ({anio_objetivo})")
                        else:
                            print(f"    -> Advertencia: Tabla de estadísticas no encontrada para {anio_objetivo}")
                            
                    except Exception as ex:
                        print(f"    -> Error en descarga {anio_objetivo}: {ex}")
                else:
                    print(f"    -> El año {anio_objetivo} no aparece disponible en el perfil web.")

        except Exception as e:
            print(f"Error accediendo al perfil: {e}")
            try: driver.current_url
            except: driver = iniciar_navegador()
        
        contador_sesion += 1

    driver.quit()
    print("\nProceso de sincronización finalizado.")

if __name__ == "__main__":
    descargar_estadisticas_faltantes()