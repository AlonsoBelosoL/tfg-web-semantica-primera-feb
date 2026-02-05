import time
import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- CONFIGURACIÓN GLOBAL ---
ANIO_INICIO = 2015 
ANIO_FIN = 2025 
RUTA_CSV_SALIDA = os.path.join("datos", "bruto", "equipos", "maestro_equipos.csv")

def iniciar_navegador():
    """Inicializa la instancia del navegador Chrome con las opciones definidas."""
    opciones = Options()
    opciones.add_argument("--start-maximized")
    # opciones.add_argument("--headless") # Ejecución en segundo plano (opcional)
    
    servicio = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=servicio, options=opciones)

def capturar_equipos_historicos():
    """
    Recorre las temporadas indicadas y extrae la lista de equipos participantes.
    Genera un archivo CSV maestro con las URLs base de cada equipo por temporada.
    """
    print("Iniciando proceso de extracción de equipos (Primera FEB / LEB Oro)...")
    
    # Crear directorio de destino si no existe
    carpeta_destino = os.path.dirname(RUTA_CSV_SALIDA)
    os.makedirs(carpeta_destino, exist_ok=True)

    driver = iniciar_navegador()
    lista_equipos = []

    try:
        for anio in range(ANIO_INICIO, ANIO_FIN + 1):
            # URL del listado de equipos para la temporada específica
            url_temporada = f"https://www.proballers.com/es/baloncesto/liga/194/spain-leb-gold/equipos/{anio}"
            
            print(f"Procesando temporada {anio}-{anio+1} | URL: {url_temporada}")
            
            driver.get(url_temporada)
            time.sleep(3) # Espera para carga completa del DOM
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            clase_tarjeta = "home-league__team-list__content__entry-team__presentation"
            enlaces_equipos = soup.find_all('a', class_=clase_tarjeta)
            
            contador_anio = 0
            
            for link in enlaces_equipos:
                nombre_equipo = link.get_text(strip=True)
                href_relativo = link['href']
                
                if nombre_equipo and href_relativo:
                    # Construcción de la URL absoluta
                    url_base = href_relativo if href_relativo.startswith("http") else f"https://www.proballers.com{href_relativo}"
                    
                    # Se fuerza la inclusión del año en la URL para evitar redirecciones automáticas a la temporada actual
                    url_completa = f"{url_base}/{anio}"
                    
                    lista_equipos.append({
                        "temporada": f"{anio}-{anio+1}",
                        "anio_inicio": anio,
                        "nombre_equipo": nombre_equipo,
                        "url_equipo": url_completa,
                        "id_liga": 194
                    })
                    contador_anio += 1
            
            print(f" -> Registrados {contador_anio} equipos para la temporada {anio}.")

    except Exception as e:
        print(f"Error crítico durante la ejecución: {e}")
    
    finally:
        print("Cerrando navegador y guardando resultados...")
        driver.quit()
        
        if lista_equipos:
            df_equipos = pd.DataFrame(lista_equipos)
            df_equipos.to_csv(RUTA_CSV_SALIDA, index=False, encoding='utf-8')
            
            print(f"Proceso finalizado. Archivo generado: {RUTA_CSV_SALIDA}")
            print(f"Total de registros: {len(df_equipos)}")
        else:
            print("No se han extraído datos. Verifique la conexión o los selectores.")

if __name__ == "__main__":
    capturar_equipos_historicos()