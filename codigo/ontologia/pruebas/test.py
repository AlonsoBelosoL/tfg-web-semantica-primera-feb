from rdflib import Graph
import os

# Rutas
ruta_script = os.path.dirname(os.path.abspath(__file__))
raiz = os.path.abspath(os.path.join(ruta_script, "..", "..", ".."))
ruta_grafo = os.path.join(raiz, "datos", "grafo", "bball_intelligence_MASTER.ttl")

print("Cargando grafo maestro (esto puede tardar unos segundos por el volumen)...")
grafo = Graph()
grafo.parse(ruta_grafo, format="turtle")

# Consulta: Top 10 jugadores TS% en 2020 con Wikidata
consulta_sparql = """
PREFIX feb: <http://www.tfg-basket.es/ontologia/primera-feb#>
PREFIX schema: <https://schema.org/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT ?nombre ?ts ?wikidata WHERE {
    ?persona a schema:Person ;
             schema:name ?nombre ;
             feb:hasPlayerAnalysis ?analisis ;
             owl:sameAs ?wikidata .
    
    ?analisis feb:tsPercentage ?ts .
    
    # Filtramos por una URI que contenga 2020 (ajusta segun tus IDs)
    FILTER(CONTAINS(STR(?analisis), "2020"))
}
ORDER BY DESC(?ts)
LIMIT 10
"""

print("--- RESULTADOS TOP 10 TS% (2020) CON ENLACE EXTERNO ---")
resultados = grafo.query(consulta_sparql)

for fila in resultados:
    print(f"Jugador: {fila.nombre} | TS%: {fila.ts}% | WD: {fila.wikidata}")