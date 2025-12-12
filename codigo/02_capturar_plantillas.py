import time
import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- CONFIGURACIÓN GLOBAL ---
RUTA_MAESTRO_EQUIPOS = os.path.join("datos", "bruto", "equipos", "maestro_equipos.csv")
RUTA_MAESTRO_PLANTILLAS = os.path.join("datos", "bruto", "plantillas", "maestro_plantillas.csv")
FRECUENCIA_REINICIO_DRIVER = 20 # Número de iteraciones antes de reiniciar el navegador para liberar memoria

def iniciar_navegador():
    opciones = Options()
    opciones.add_argument("--start-maximized")
    # opciones.add_argument("--headless")
    servicio = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=servicio, options=opciones)
    driver.set_page_load_timeout(30)
    return driver

def extraer_plantillas():
    """
    Lee el maestro de equipos y visita cada URL para extraer la lista de jugadores.
    Implementa lógica incremental para no reprocesar equipos ya capturados.
    """
    print("Iniciando extracción detallada de plantillas...")
    
    if not os.path.exists(RUTA_MAESTRO_EQUIPOS):
        print(f"Error: No se encuentra el archivo de entrada {RUTA_MAESTRO_EQUIPOS}")
        return

    df_equipos = pd.read_csv(RUTA_MAESTRO_EQUIPOS)
    print(f"Total de equipos a procesar: {len(df_equipos)}")

    # Lógica de recuperación: Identificar equipos ya procesados
    equipos_procesados = set()
    if os.path.exists(RUTA_MAESTRO_PLANTILLAS):
        try:
            df_existente = pd.read_csv(RUTA_MAESTRO_PLANTILLAS)
            # Crear clave única (Equipo + Temporada) para el filtrado
            for _, fila in df_existente.iterrows():
                clave = f"{fila['nombre_equipo']}_{fila['temporada']}"
                equipos_procesados.add(clave)
            print(f"Se han detectado {len(equipos_procesados)} equipos previamente procesados.")
        except Exception:
            pass

    driver = iniciar_navegador()
    buffer_jugadores = []
    contador_sesion = 0

    try:
        for indice, fila in df_equipos.iterrows():
            nombre_equipo = fila['nombre_equipo']
            temporada = fila['temporada']
            url_equipo = fila['url_equipo']
            
            clave_actual = f"{nombre_equipo}_{temporada}"
            
            # Saltar si ya existe
            if clave_actual in equipos_procesados:
                continue

            # Gestión de memoria: Reinicio periódico del driver
            if contador_sesion > 0 and contador_sesion % FRECUENCIA_REINICIO_DRIVER == 0:
                print("Reiniciando instancia del navegador para liberar recursos...")
                driver.quit()
                time.sleep(2)
                driver = iniciar_navegador()

            print(f"[{indice+1}/{len(df_equipos)}] Procesando: {nombre_equipo} ({temporada})")
            
            try:
                driver.get(url_equipo)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Extracción de datos: Se busca en las tablas que contienen estadísticas
                filas_tabla = soup.find_all('tr')
                
                jugadores_encontrados_equipo = 0
                urls_procesadas_local = set()
                
                for fila in filas_tabla:
                    enlace = fila.find('a', href=True)
                    
                    # Validar que el enlace corresponde a un perfil de jugador
                    if enlace and ('/jugador/' in enlace['href'] or '/player/' in enlace['href']):
                        # Verificar que la fila contiene datos estadísticos (columnas suficientes)
                        cols = fila.find_all('td')
                        if len(cols) > 5:
                            nombre_jugador = enlace.get_text(strip=True)
                            href_jugador = enlace['href']
                            
                            url_absoluta = href_jugador if href_jugador.startswith("http") else f"https://www.proballers.com{href_jugador}"
                            
                            if url_absoluta not in urls_procesadas_local:
                                buffer_jugadores.append({
                                    "temporada": temporada,
                                    "nombre_equipo": nombre_equipo,
                                    "nombre_jugador": nombre_jugador,
                                    "url_jugador": url_absoluta,
                                    "url_equipo_origen": url_equipo
                                })
                                urls_procesadas_local.add(url_absoluta)
                                jugadores_encontrados_equipo += 1
                
                print(f" -> Extraídos {jugadores_encontrados_equipo} jugadores.")
                
                # Escritura incremental en disco cada 5 equipos
                if contador_sesion % 5 == 0:
                    modo_escritura = 'a' if os.path.exists(RUTA_MAESTRO_PLANTILLAS) else 'w'
                    escribir_cabecera = not os.path.exists(RUTA_MAESTRO_PLANTILLAS)
                    
                    if buffer_jugadores:
                        pd.DataFrame(buffer_jugadores).to_csv(RUTA_MAESTRO_PLANTILLAS, mode=modo_escritura, header=escribir_cabecera, index=False)
                        buffer_jugadores = [] # Vaciar buffer
                        
            except Exception as e:
                print(f"Error procesando equipo {nombre_equipo}: {e}")
                # Intentar recuperar el navegador si ha fallado
                try: 
                    driver.current_url
                except: 
                    driver = iniciar_navegador()
            
            contador_sesion += 1

    finally:
        driver.quit()
        # Guardado final de datos pendientes en el buffer
        if buffer_jugadores:
            modo_escritura = 'a' if os.path.exists(RUTA_MAESTRO_PLANTILLAS) else 'w'
            escribir_cabecera = not os.path.exists(RUTA_MAESTRO_PLANTILLAS)
            pd.DataFrame(buffer_jugadores).to_csv(RUTA_MAESTRO_PLANTILLAS, mode=modo_escritura, header=escribir_cabecera, index=False)
            print("Escritura final completada.")

if __name__ == "__main__":
    extraer_plantillas()