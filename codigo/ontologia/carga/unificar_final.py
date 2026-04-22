from rdflib import Graph
import os

# Configuracion de rutas
ruta_script = os.path.dirname(os.path.abspath(__file__))
directorio_raiz = os.path.abspath(os.path.join(ruta_script, "..", "..", ".."))
carpeta_grafo = os.path.join(directorio_raiz, "datos", "grafo")

# Lista de archivos que componen el grafo completo
archivos_entrada = [
    "capa1_maestros.ttl", 
    "capa2_eventos.ttl", 
    "capa3_analisis.ttl",
    "interlinking_wikidata.ttl"
]

grafo_maestro = Graph()

print("--- Iniciando Unificacion Final del Grafo con Enlaces Externos ---")

for nombre_archivo in archivos_entrada:
    ruta_completa = os.path.join(carpeta_grafo, nombre_archivo)
    if os.path.exists(ruta_completa):
        print(f"Añadiendo: {nombre_archivo}...")
        grafo_maestro.parse(ruta_completa, format="turtle")
    else:
        print(f"Aviso: No se encontro el archivo {nombre_archivo}")

# Guardar el resultado final
ruta_salida_master = os.path.join(carpeta_grafo, "bball_intelligence_MASTER.ttl")
grafo_maestro.serialize(destination=ruta_salida_master, format="turtle")

print("-" * 30)
print(f"PROCESO COMPLETADO CON EXITO")
print(f"Total de tripletas en el grafo final: {len(grafo_maestro)}")
print(f"Archivo maestro generado en: {ruta_salida_master}")