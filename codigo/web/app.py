from flask import Flask, render_template, request
from rdflib import Graph, Namespace, URIRef
import os

app = Flask(__name__)

# Rutas de archivos
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_grafo_maestro = os.path.abspath(os.path.join(directorio_actual, "../../datos/grafo/bball_intelligence_MASTER.ttl"))

# Namespaces
FEB = Namespace("http://www.tfg-basket.es/ontologia/primera-feb#")
RES = Namespace("https://bball-intelligence.com/resource/")
SCHEMA = Namespace("https://schema.org/")
OWL = Namespace("http://www.w3.org/2002/07/owl#")

grafo_baloncesto = Graph()
print("Cargando base de datos semantica...")
if os.path.exists(ruta_grafo_maestro):
    grafo_baloncesto.parse(ruta_grafo_maestro, format="turtle")
    print("Grafo cargado exitosamente.")

@app.route('/')
def inicio():
    consulta_conteo = "SELECT (COUNT(?p) AS ?total) WHERE { ?p a <https://schema.org/Person> }"
    resultado = grafo_baloncesto.query(consulta_conteo)
    total_jugadores = str(list(resultado)[0][0])
    return render_template('inicio.html', cantidad=total_jugadores)

@app.route('/jugadores')
def listar_jugadores():
    busqueda = request.args.get('nombre', '')
    filtro = f'FILTER(regex(str(?nombre), "{busqueda}", "i"))' if busqueda else ""
    
    consulta = f"""
    PREFIX schema: <https://schema.org/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?uri ?nombre ?wikidata WHERE {{
        ?uri a schema:Person ; schema:name ?nombre .
        {filtro}
        OPTIONAL {{ ?uri owl:sameAs ?wikidata . }}
    }} ORDER BY ?nombre LIMIT 50
    """
    
    resultados = grafo_baloncesto.query(consulta)
    lista_jugadores = []
    for fila in resultados:
        lista_jugadores.append({
            'id': str(fila.uri).split('/')[-1],
            'nombre': str(fila.nombre),
            'wikidata': str(fila.wikidata) if fila.wikidata else None
        })
    return render_template('jugadores.html', jugadores=lista_jugadores, busqueda=busqueda)

@app.route('/jugador/<id_jugador>')
def detalle_jugador(id_jugador):
    uri_sujeto = URIRef(f"https://bball-intelligence.com/resource/person/{id_jugador}")
    
    # Datos de perfil y avanzada
    consulta_perfil = f"""
    PREFIX schema: <https://schema.org/>
    PREFIX feb: <http://www.tfg-basket.es/ontologia/primera-feb#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?nombre ?url_pb ?wikidata ?ts ?efg ?val_min ?ortg WHERE {{
        <{uri_sujeto}> schema:name ?nombre ; schema:url ?url_pb .
        OPTIONAL {{ <{uri_sujeto}> owl:sameAs ?wikidata . }}
        OPTIONAL {{
            <{uri_sujeto}> feb:hasPlayerAnalysis ?analisis .
            OPTIONAL {{ ?analisis feb:tsPercentage ?ts . }}
            OPTIONAL {{ ?analisis feb:efgPercentage ?efg . }}
            OPTIONAL {{ ?analisis feb:valPerMinute ?val_min . }}
            OPTIONAL {{ ?analisis feb:ortgIndividual ?ortg . }}
        }}
    }} ORDER BY DESC(?analisis) LIMIT 1
    """
    
    # Actuaciones recientes
    consulta_partidos = f"""
    PREFIX feb: <http://www.tfg-basket.es/ontologia/primera-feb#>
    SELECT ?fecha ?puntos ?valoracion WHERE {{
        ?act feb:performer <{uri_sujeto}> ;
             feb:playedMatch ?partido ;
             feb:points ?puntos ;
             feb:efficiencyValue ?valoracion .
        ?partido feb:startDate ?fecha .
    }} ORDER BY DESC(?fecha) LIMIT 10
    """

    res_p = grafo_baloncesto.query(consulta_perfil)
    res_m = grafo_baloncesto.query(consulta_partidos)

    datos = {}
    for f in res_p:
        datos = {
            'nombre': str(f.nombre),
            'url_pb': str(f.url_pb),
            'wikidata': str(f.wikidata) if f.wikidata else None,
            'ts': round(float(f.ts), 2) if f.ts else None,
            'efg': round(float(f.efg), 2) if f.efg else None,
            'val_min': round(float(f.val_min), 2) if f.val_min else None,
            'ortg': round(float(f.ortg), 2) if f.ortg else None
        }

    partidos = []
    for f in res_m:
        partidos.append({
            'fecha': str(f.fecha),
            'puntos': int(float(f.puntos)),
            'valoracion': int(float(f.valoracion))
        })

    return render_template('jugador.html', jugador=datos, partidos=partidos)

if __name__ == '__main__':
    app.run(debug=True, port=5000)