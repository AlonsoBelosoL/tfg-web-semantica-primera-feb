import pandas as pd
import requests
import time
import os
from rdflib import Graph, Namespace, URIRef

# Definimos los Namespaces manualmente para evitar errores de importacion
RES = Namespace("https://bball-intelligence.com/resource/")
SCHEMA = Namespace("https://schema.org/")
URL_SPARQL = "https://query.wikidata.org/sparql"

# Cabecera necesaria para Wikidata
CABECERAS = {'User-Agent': 'TFG_Basket_Enrichment/1.0 (alonsoucles@gmail.com)'}

def obtener_foto_wikidata(qid):
    """Consulta la URL de la imagen (P18) en Wikidata."""
    consulta = f"""
    SELECT ?foto WHERE {{
      wd:{qid} wdt:P18 ?foto .
    }}
    """
    try:
        respuesta = requests.get(URL_SPARQL, params={'query': consulta, 'format': 'json'}, headers=CABECERAS, timeout=10)
        datos = respuesta.json()
        resultados = datos.get("results", {}).get("bindings", [])
        if resultados:
            return resultados[0]["foto"]["value"]
    except:
        return None
    return None

# 2. Rutas
ruta_script = os.path.dirname(os.path.abspath(__file__))
raiz = os.path.abspath(os.path.join(ruta_script, "..", "..", ".."))
ruta_interlinking = os.path.join(raiz, "datos", "grafo", "interlinking_wikidata.ttl")
ruta_salida = os.path.join(raiz, "datos", "grafo", "enriquecimiento_fotos.ttl")

# 3. Ejecucion
print("--- Iniciando Enriquecimiento de Imagenes desde Wikidata ---")
grafo_fotos = Graph()
grafo_fotos.bind("res", RES)
grafo_fotos.bind("schema", SCHEMA)

# Cargamos solo los enlaces para saber a quien buscar
grafo_enlaces = Graph()
grafo_enlaces.parse(ruta_interlinking, format="turtle")

# Buscamos todos los sujetos que tengan un owl:sameAs
query_enlaces = """
SELECT ?sujeto ?objeto WHERE {
    ?sujeto <http://www.w3.org/2002/07/owl#sameAs> ?objeto .
}
"""

enlaces = list(grafo_enlaces.query(query_enlaces))
total = len(enlaces)
fotos_encontradas = 0

for indice, (sujeto, objeto) in enumerate(enlaces):
    qid = str(objeto).split('/')[-1]
    nombre_consola = str(sujeto).split('/')[-1]
    
    print(f"[{indice+1}/{total}] Buscando foto para {nombre_consola}...", end=" ")
    
    url_foto = obtener_foto_wikidata(qid)
    
    if url_foto:
        # Añadimos la propiedad schema:image al jugador
        grafo_fotos.add((sujeto, SCHEMA.image, URIRef(url_foto)))
        fotos_encontradas += 1
        print("[FOTO OK]")
    else:
        print("[SIN FOTO]")
    
    # Respetar limites de API
    time.sleep(0.3)

# 4. Guardar
os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
grafo_fotos.serialize(destination=ruta_salida, format="turtle")

print(f"\nProceso finalizado. Fotos añadidas: {fotos_encontradas}")