import pandas as pd
import re

def limpiar_url_equipo(url):
    if pd.isna(url):
        return url
    return re.sub(r'/\d{4}$', '', url)

def procesar_capa1():
    # 1. Carga de los datos brutos
    # (Asegúrate de que estas rutas existen en tu repo local/codespace)
    copia_equipos_bruto = pd.read_csv('datos/bruto/equipos/maestro_equipos.csv')
    copia_plantillas_bruto = pd.read_csv('datos/bruto/plantillas/maestro_plantillas.csv')

    # --- LIMPIEZA INICIAL DE STRINGS ---
    # Quitamos espacios en blanco extra en columnas de texto
    for df in [copia_equipos_bruto, copia_plantillas_bruto]:
        text_cols = df.select_dtypes(include=['object']).columns
        df[text_cols] = df[text_cols].apply(lambda x: x.str.strip())

    # Diccionario para correcciones manuales (Ejemplo Ourense)
    correcciones_equipos = {
        'Ourence': 'Ourense',
        'C.B. Prat': 'CB Prat' # Ejemplo de normalización
    }
    copia_equipos_bruto['nombre_equipo'] = copia_equipos_bruto['nombre_equipo'].replace(correcciones_equipos)
    copia_plantillas_bruto['nombre_equipo'] = copia_plantillas_bruto['nombre_equipo'].replace(correcciones_equipos)

    # --- BLOQUE 1: EQUIPOS ---
    
    copia_equipos_bruto['uri_equipo'] = copia_equipos_bruto['url_equipo'].apply(limpiar_url_equipo)

    # capa1_equipos.csv: Identidad única del Club
    capa1_lista_equipos = copia_equipos_bruto[['uri_equipo', 'nombre_equipo']].drop_duplicates(subset=['uri_equipo'], keep='first')
    # Ordenar alfabéticamente por nombre
    capa1_lista_equipos = capa1_lista_equipos.sort_values(by='nombre_equipo')

    # capa1_equipos_temporada.csv
    capa1_lista_equipos_temporadas = copia_equipos_bruto[['uri_equipo', 'url_equipo', 'temporada', 'anio_inicio', 'nombre_equipo', 'id_liga']].copy()
    capa1_lista_equipos_temporadas = capa1_lista_equipos_temporadas.rename(columns={
        'url_equipo': 'uri_equipo_temporada',
        'anio_inicio': 'ano_inicio'
    })
    # ORDENACIÓN: Por año y luego por nombre
    capa1_lista_equipos_temporadas = capa1_lista_equipos_temporadas.sort_values(by=['ano_inicio', 'nombre_equipo'])

    # --- BLOQUE 2: JUGADORES Y PLANTILLAS ---

    # capa1_jugadores.csv: Identidad única del Jugador
    capa1_lista_jugadores = copia_plantillas_bruto[['url_jugador', 'nombre_jugador']].drop_duplicates(subset=['url_jugador'], keep='first')
    capa1_lista_jugadores = capa1_lista_jugadores.sort_values(by='nombre_jugador')

    # capa1_plantillas.csv: Relaciones
    copia_plantillas_bruto['anio_inicio'] = copia_plantillas_bruto['temporada'].str.split('-').str[0].astype(int)
    copia_plantillas_bruto['uri_equipo'] = copia_plantillas_bruto['url_equipo_origen'].apply(limpiar_url_equipo)

    # Solo las columnas necesarias. Mantenemos anio_inicio como int.
    capa1_plantillas = copia_plantillas_bruto[['url_jugador', 'uri_equipo', 'temporada', 'anio_inicio']].copy()
    
    # ORDENACIÓN: Agrupamos por temporada y luego por equipo para que la plantilla esté junta
    capa1_plantillas = capa1_plantillas.sort_values(by=['anio_inicio', 'uri_equipo', 'url_jugador'])

    # 2. Exportación
    # Usamos float_format=None para asegurar que los ints no lleven .0
    capa1_lista_equipos.to_csv('datos/procesados/capa1/capa1_equipos.csv', index=False)
    capa1_lista_equipos_temporadas.to_csv('datos/procesados/capa1/capa1_equipos_temporada.csv', index=False)
    capa1_lista_jugadores.to_csv('datos/procesados/capa1/capa1_jugadores.csv', index=False)
    capa1_plantillas.to_csv('datos/procesados/capa1/capa1_plantillas.csv', index=False)

    print("Capa 1 completada con éxito y datos ordenados.")

if __name__ == "__main__":
    procesar_capa1()