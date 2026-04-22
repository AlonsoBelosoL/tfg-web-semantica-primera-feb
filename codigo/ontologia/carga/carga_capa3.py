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
dir_capa3 = os.path.join(root_dir, "datos", "procesados", "capa3")
ruta_grafo = os.path.join(root_dir, "datos", "grafo")
ruta_salida = os.path.join(ruta_grafo, "capa3_analisis.ttl")

g = Graph()
g.bind("feb", FEB); g.bind("res", RES); g.bind("schema", SCHEMA)

def extraer_id(url):
    match = re.search(r'/(\d+)/?', str(url))
    return match.group(1) if match else "desconocido"

print(f"--- Cargando Capa 3: Inteligencia Estadística ---")

try:
    # --- A. ANÁLISIS DE EQUIPOS (feb:TeamAnalysis) ---
    print("Procesando análisis avanzado de equipos...")
    df_eq_adv = pd.read_csv(os.path.join(dir_capa3, 'capa3_equipos_avanzado.csv'))
    for _, fila in df_eq_adv.iterrows():
        c_id = extraer_id(fila['uri_equipo'])
        year = str(fila['ano_inicio'])
        
        # URI del equipo-temporada (para enlazar el análisis)
        uri_ts = RES[f"team-season/{c_id}_{year}"]
        uri_analysis = RES[f"team-analysis/{c_id}_{year}"]
        
        g.add((uri_analysis, RDF.type, FEB.TeamAnalysis))
        g.add((uri_ts, FEB.hasTeamAnalysis, uri_analysis))
        
        # Mapeo de métricas avanzadas de equipo
        metricas_eq = {
            'victorias_total': FEB.totalWins,
            'win_rate': FEB.winRate,
            'ts_porcentaje': FEB.tsPercentage,
            'ast_ratio': FEB.astRatio,
            'reb_ratio_ofensivo': FEB.offRebRatio,
            'posesiones_totales': FEB.totalPossessions,
            'posesiones_por_partido': FEB.pace,
            'ortg_equipo': FEB.ortgTeam
        }
        
        for col, predicado in metricas_eq.items():
            if pd.notnull(fila[col]):
                tipo = XSD.integer if 'total' in col else XSD.float
                g.add((uri_analysis, predicado, Literal(fila[col], datatype=tipo)))

    # --- B. ANÁLISIS DE JUGADORES (feb:PlayerAnalysis) ---
    print("Procesando análisis avanzado de jugadores...")
    df_jug_adv = pd.read_csv(os.path.join(dir_capa3, 'capa3_jugadores_avanzado.csv'))
    for _, fila in df_jug_adv.iterrows():
        p_id = extraer_id(fila['url_jugador'])
        c_id = extraer_id(fila['uri_equipo'])
        year = str(fila['ano_inicio'])
        
        uri_persona = RES[f"person/{p_id}"]
        uri_analysis = RES[f"player-analysis/{p_id}_{c_id}_{year}"]
        
        g.add((uri_analysis, RDF.type, FEB.PlayerAnalysis))
        g.add((uri_persona, FEB.hasPlayerAnalysis, uri_analysis))
        
        # Mapeo de métricas avanzadas de jugador
        metricas_jug = {
            'ts_porcentaje': FEB.tsPercentage,
            'efg_porcentaje': FEB.efgPercentage,
            'ratio_ast_to': FEB.astToRatio,
            'valoracion_por_minuto': FEB.valPerMinute,
            'posesiones_terminadas': FEB.finishedPossessions,
            'ortg_individual': FEB.ortgIndividual,
            'minutos_total': FEB.totalMinutes,
            'minutos_promedio': FEB.avgMinutes,
            'es_doble_doble_total': FEB.totalDoubleDoubles
        }
        
        for col, predicado in metricas_jug.items():
            if pd.notnull(fila[col]):
                tipo = XSD.integer if 'total' in col else XSD.float
                g.add((uri_analysis, predicado, Literal(fila[col], datatype=tipo)))

    # --- 3. GUARDADO ---
    os.makedirs(ruta_grafo, exist_ok=True)
    g.serialize(destination=ruta_salida, format="turtle")
    print(f"--- ÉXITO: Capa 3 generada con {len(g)} tripletas en {ruta_salida} ---")

except Exception as e:
    print(f"Error en Capa 3: {e}")