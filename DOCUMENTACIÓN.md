-- CÓDIGO --

-Web scrapping-

01_capturar_equipos.py -> URL base para cada tupla [temporada, equipo]

02_capturar_plantillas.py -> Del índice de equipos generado en el paso anterior extrae la lista de jugadores de cada plantilla. URL para tripleta [temporada, equipo, jugador]

03_capturar_jugadores.py -> Utiliza el listado de plantillas para descargar las estadísticas individuales de cada partido. Estadísticas de cada jornada, de cada jugador, de cada plantilla, de cada temporada.

-Limpieza-
capa1.py -> A partir de maestro_equipos.csv obtenemos 2 csv:
  -> capa1_equipos.csv : contiene tuplas [url_equipo, nombre_equipo] sin repetición.
  -> capa1_equipos_temporada: agrupados por temporada, todos los equipos que participaron en Primera Feb.
                              Por fila -> [url_equipo, url_equipo_temporada, temporada, ano_inicio, nombre_equipo, id_liga]
A partir de maestro_plantillas.csv obtenemos 2 csv:
  -> capa1_jugadores.csv: contiene tuplas [url_jugador, nombre_jugador] sin repetición.
  -> capa1_plantillas.csv: agrupados por temporada y equipo, contiene para cada jugador [url_jugador, url_equipo, temporada, ano_inicio]

Cada archivo corrige el orden, de modo que se ordenan por temporada y por orden alfabético. También corrige errores gramaticales y espacios en blanco innecesarios.
Esta capa es importante para diferenciar Identidad (recursos únicos) de Contexto (acciones de ese contexto).

-- DATOS --

-Bruto-
Obtenidos directamente de hacer scrapping. Están desordenados, porque a veces la página fallaba a la hora de coger ciertos datos y hubo que añadirlos después.

datos/bruto/equipos/maestro_equipos.csv: URL base para cada tupla [temporada, equipo]

datos/bruto/plantillas/maestro_plantillas.csv: URL para tripleta [temporada, equipo, jugador]

datos/bruto/temporadas/: Estadísticas de cada partido de cada jugador.
Temporada (Ej: 2015-2016)

Equipo (Ej: Palencia_Baloncesto)

Archivos CSV: Cada archivo corresponde a un jugador de esa plantilla y contiene sus estadísticas desglosadas partido a partido.                        

-Procesados-

Temporada (Ej: 2015-2016)

Equipo (Ej: Palencia_Baloncesto)

Archivos CSV: Cada archivo corresponde a un jugador de esa plantilla y contiene sus estadísticas desglosadas partido a partido.
