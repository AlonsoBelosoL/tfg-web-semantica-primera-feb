import pandas as pd
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD
import re
import os

# 1. Configuración de Namespaces (ESTRICTO SEGÚN TU .TTL)
FEB = Namespace("http://www.tfg-basket.es/ontologia/primera-feb#")
RES = Namespace("https://bball-intelligence.com/resource/")
SCHEMA = Namespace("https://schema.org/") 

# 2. Lógica de Rutas
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
base_datos = os.path.join(root_dir, "datos", "procesados", "capa1")
ruta_grafo = os.path.join(root_dir, "datos", "grafo")
ruta_salida = os.path.join(ruta_grafo, "capa1_maestros.ttl")

g = Graph()
g.bind("feb", FEB)
g.bind("res", RES)
g.bind("schema", SCHEMA)

def extraer_id(url):
    match = re.search(r'/(\d+)/?', str(url))
    return match.group(1) if match else "desconocido"

print(f"--- Iniciando carga de Capa 1: Sincronizada con Ontología ---")

try:
    # --- A. JUGADORES (schema:Person) ---
    print("Procesando jugadores...")
    df_jug = pd.read_csv(os.path.join(base_datos, 'capa1_jugadores.csv'))
    for _, row in df_jug.iterrows():
        p_id = extraer_id(row['url_jugador'])
        uri_person = RES[f"person/{p_id}"]
        g.add((uri_person, RDF.type, SCHEMA.Person))
        g.add((uri_person, SCHEMA.name, Literal(row['nombre_jugador'], datatype=XSD.string)))
        g.add((uri_person, SCHEMA.url, URIRef(row['url_jugador'])))

    # --- B. CLUBES (schema:SportsOrganization) ---
    print("Procesando clubes...")
    df_eq = pd.read_csv(os.path.join(base_datos, 'capa1_equipos.csv'))
    for _, row in df_eq.iterrows():
        c_id = extraer_id(row['uri_equipo'])
        uri_club = RES[f"club/{c_id}"]
        g.add((uri_club, RDF.type, SCHEMA.SportsOrganization))
        g.add((uri_club, SCHEMA.name, Literal(row['nombre_equipo'], datatype=XSD.string)))
        g.add((uri_club, SCHEMA.url, URIRef(row['uri_equipo'])))

    # --- C. LIGAS, TEMPORADAS Y EQUIPOS-TEMPORADA (feb:...) ---
    print("Procesando ligas y temporadas...")
    df_et = pd.read_csv(os.path.join(base_datos, 'capa1_equipos_temporada.csv'))
    for _, row in df_et.iterrows():
        c_id = extraer_id(row['uri_equipo'])
        year, liga_id = str(row['ano_inicio']), str(row['id_liga'])
        
        uri_season = RES[f"season/{year}"]
        uri_league = RES[f"league/{liga_id}"]
        uri_ts = RES[f"team-season/{c_id}_{year}"]
        
        g.add((uri_season, RDF.type, FEB.Season))
        g.add((uri_season, FEB.startYear, Literal(row['ano_inicio'], datatype=XSD.integer)))
        
        g.add((uri_league, RDF.type, FEB.League))
        g.add((uri_league, FEB.leagueId, Literal(liga_id, datatype=XSD.string)))
        
        g.add((uri_ts, RDF.type, FEB.TeamSeason))
        g.add((uri_ts, FEB.teamName, Literal(row['nombre_equipo'], datatype=XSD.string)))
        
        # Relaciones (Ojo al BelongsToClub con B mayúscula del .ttl)
        g.add((uri_ts, FEB.BelongsToClub, RES[f"club/{c_id}"]))
        g.add((uri_ts, FEB.duringSeason, uri_season))
        g.add((uri_ts, FEB.inLeague, uri_league))

    # --- D. PLANTILLAS (feb:RosterItem) ---
    print("Procesando plantillas...")
    df_pl = pd.read_csv(os.path.join(base_datos, 'capa1_plantillas.csv'))
    for _, row in df_pl.iterrows():
        p_id = extraer_id(row['url_jugador'])
        c_id = extraer_id(row['uri_equipo'])
        year = str(row['anio_inicio'])
        
        uri_roster = RES[f"roster/{p_id}_{c_id}_{year}"]
        g.add((uri_roster, RDF.type, FEB.RosterItem))
        
        # Unimos Person -> RosterItem -> TeamSeason
        g.add((RES[f"person/{p_id}"], FEB.hasRosterItem, uri_roster))
        g.add((uri_roster, FEB.rosterInTeam, RES[f"team-season/{c_id}_{year}"]))

    # --- 3. GUARDADO ---
    os.makedirs(ruta_grafo, exist_ok=True)
    g.serialize(destination=ruta_salida, format="turtle")
    print(f"--- ÉXITO: {len(g)} tripletas guardadas en {ruta_salida} ---")

except Exception as e:
    print(f"Error en Capa 1: {e}")