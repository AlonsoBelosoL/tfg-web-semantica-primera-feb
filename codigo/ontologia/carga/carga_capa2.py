import pandas as pd
from rdflib import Graph, Literal, RDF, Namespace
from rdflib.namespace import XSD
import re
import os

# 1. Configuración de Namespaces (SEGÚN TU .TTL)
FEB = Namespace("http://www.tfg-basket.es/ontologia/primera-feb#")
RES = Namespace("https://bball-intelligence.com/resource/")
SCHEMA = Namespace("https://schema.org/")

# 2. Lógica de Rutas
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
dir_capa2 = os.path.join(root_dir, "datos", "procesados", "capa2")
ruta_grafo = os.path.join(root_dir, "datos", "grafo")
ruta_salida = os.path.join(ruta_grafo, "capa2_eventos.ttl")

g = Graph()
g.bind("feb", FEB)
g.bind("res", RES)
g.bind("schema", SCHEMA)

def extraer_id(url):
    match = re.search(r'/(\d+)/?', str(url))
    return match.group(1) if match else "desconocido"

print(f"--- Cargando Capa 2 siguiendo tu Ontología ---")

try:
    # --- A. PARTIDOS (schema:SportsEvent) ---
    print("Procesando partidos...")
    df_partidos = pd.read_csv(os.path.join(dir_capa2, 'capa2_partidos.csv'))
    for _, fila in df_partidos.iterrows():
        uri_partido = RES[f"match/{fila['id_partido']}"]
        
        g.add((uri_partido, RDF.type, SCHEMA.SportsEvent))
        g.add((uri_partido, FEB.startDate, Literal(fila['fecha'], datatype=XSD.date)))
        g.add((uri_partido, FEB.matchday, Literal(fila['jornada'], datatype=XSD.integer)))
        g.add((uri_partido, FEB.duringSeason, RES[f"season/{fila['ano_inicio']}"]))
        
        # Mapeo de equipos según tu ontología
        id_local = extraer_id(fila['uri_local'])
        id_visitante = extraer_id(fila['uri_visitante'])
        g.add((uri_partido, FEB.homeTeam, RES[f"club/{id_local}"]))
        g.add((uri_partido, FEB.awayTeam, RES[f"club/{id_visitante}"]))

    # --- B. ACTUACIONES (feb:MatchPerformance) ---
    print("Procesando estadísticas detalladas...")
    df_stats = pd.read_csv(os.path.join(dir_capa2, 'capa2_estadisticas_detalladas.csv'))
    
    for _, fila in df_stats.iterrows():
        p_id = extraer_id(fila['url_jugador'])
        m_id = str(fila['id_partido'])
        uri_perf = RES[f"performance/{m_id}_{p_id}"]
        
        g.add((uri_perf, RDF.type, FEB.MatchPerformance))
        g.add((uri_perf, FEB.performer, RES[f"person/{p_id}"]))
        g.add((uri_perf, FEB.playedMatch, RES[f"match/{m_id}"]))
        
        # MAPEO EXACTO A TUS DATA PROPERTIES
        mapeo = {
            'minutos': FEB.minutes,
            'puntos': FEB.points,
            'valoracion': FEB.efficiencyValue,
            't2_metidos': FEB.t2Made,
            't2_intentados': FEB.t2Attempted,
            't3_metidos': FEB.t3Made,
            't3_intentados': FEB.t3Attempted,
            't1_metidos': FEB.t1Made,
            't1_intentados': FEB.t1Attempted,
            'rebotes_ofensivos': FEB.offRebounds,
            'rebotes_defensivos': FEB.defRebounds,
            'rebotes_totales': FEB.totalRebounds,
            'asistencias': FEB.assists,
            'robos': FEB.steals,
            'tapones': FEB.blocks,
            'perdidas': FEB.turnovers,
            'faltas_cometidas': FEB.foulsCommitted,
            'faltas_recibidas': FEB.foulsReceived,
            'mas_menos': FEB.plusMinus
        }

        for col, predicado in mapeo.items():
            if pd.notnull(fila[col]):
                # plusMinus es float en tu ontología
                tipo = XSD.float if col == 'mas_menos' else XSD.integer
                g.add((uri_perf, predicado, Literal(fila[col], datatype=tipo)))

    # --- 4. GUARDADO ---
    os.makedirs(ruta_grafo, exist_ok=True)
    g.serialize(destination=ruta_salida, format="turtle")
    print(f"--- ÉXITO: {len(g)} tripletas guardadas en {ruta_salida} ---")

except Exception as e:
    print(f"Error: {e}")