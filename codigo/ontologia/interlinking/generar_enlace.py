import pandas as pd
import requests
import time
import os
import re
from rdflib import Graph, URIRef, Namespace, OWL

# 1. Configuracion de Namespaces y URLs
RES = Namespace("https://bball-intelligence.com/resource/")
URL_SPARQL = "https://query.wikidata.org/sparql"
URL_API = "https://www.wikidata.org/w/api.php"

# Cabecera para evitar bloqueos de Wikidata
CABECERAS = {'User-Agent': 'TFG_Basket_Bot/1.0 (alonsoucles@gmail.com)'}

def buscar_jugador_en_wikidata(id_proballers, nombre):
    # ESTRATEGIA 1: Busqueda por ID de Proballers (Propiedad P8856 en Wikidata)
    consulta_id = f"""
    SELECT ?item WHERE {{
      ?item wdt:P8856 "{id_proballers}".
    }}
    """
    try:
        respuesta = requests.get(URL_SPARQL, params={'query': consulta_id, 'format': 'json'}, headers=CABECERAS, timeout=10)
        datos = respuesta.json()
        resultados = datos.get("results", {}).get("bindings", [])
        if resultados:
            return resultados[0]["item"]["value"]
    except:
        pass

    # ESTRATEGIA 2: Busqueda por nombre a traves de la API (mas flexible con acentos)
    parametros_busqueda = {
        "action": "wbsearchentities",
        "search": nombre,
        "language": "es",
        "format": "json",
        "limit": 5
    }
    try:
        respuesta_api = requests.get(URL_API, params=parametros_busqueda, headers=CABECERAS, timeout=10)
        candidatos = respuesta_api.json().get("search", [])
        
        terminos_baloncesto = ["basket", "baloncesto", "pívot", "alero", "base", "nba", "acb", "feb", "deportista", "sport", "player"]

        for c in candidatos:
            descripcion = c.get("description", "").lower()
            # Si el ID coincide o la descripcion tiene palabras clave, lo aceptamos
            if any(t in descripcion for t in terminos_baloncesto) or not descripcion:
                return f"http://www.wikidata.org/entity/{c['id']}"
    except:
        pass

    return None

# 2. Rutas
ruta_script = os.path.dirname(os.path.abspath(__file__))
raiz = os.path.abspath(os.path.join(ruta_script, "..", "..", ".."))
ruta_csv = os.path.join(raiz, "datos", "procesados", "capa1", "capa1_jugadores.csv")
ruta_salida = os.path.join(raiz, "datos", "grafo", "interlinking_wikidata.ttl")

# 3. Ejecucion
print("--- Iniciando Interlinking (Estrategia de Maximo Alcance) ---")
grafo_enlaces = Graph()
grafo_enlaces.bind("res", RES)
grafo_enlaces.bind("owl", OWL)

try:
    tabla_jugadores = pd.read_csv(ruta_csv)
    total = len(tabla_jugadores)
    exitos = 0
    fallos_consecutivos = 0

    for indice, fila in tabla_jugadores.iterrows():
        nombre_jugador = str(fila['nombre_jugador']).strip()
        url_jugador = fila['url_jugador']
        
        # Extraer ID numerico de la URL
        busqueda_id = re.search(r'/(\d+)/?', str(url_jugador))
        id_numerico = busqueda_id.group(1) if busqueda_id else None
        
        if id_numerico:
            uri_local = RES[f"person/{id_numerico}"]
            print(f"[{indice+1}/{total}] {nombre_jugador[:25]:<25}", end=" ")
            
            enlace_wd = buscar_jugador_en_wikidata(id_numerico, nombre_jugador)
            
            if enlace_wd:
                grafo_enlaces.add((uri_local, OWL.sameAs, URIRef(enlace_wd)))
                exitos += 1
                fallos_consecutivos = 0
                print(f"[OK] -> {enlace_wd.split('/')[-1]}")
            else:
                fallos_consecutivos += 1
                print("[FALLO]")

            # Parada de seguridad si los 10 primeros fallan o si hay una racha de 10 fallos
            if fallos_consecutivos >= 10:
                print("\n--- CONTROL DE CALIDAD: 10 fallos seguidos. Revisando proceso... ---")
                break
            
            # Pausa para no saturar servidores
            time.sleep(0.4)

    # Guardar resultados
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    grafo_enlaces.serialize(destination=ruta_salida, format="turtle")
    print(f"\nProceso finalizado. Enlaces creados: {exitos}")

except Exception as e:
    print(f"Error critico: {e}")