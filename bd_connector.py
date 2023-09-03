import pytz
import api_connector as api
from datetime import datetime, timedelta
from pymongo import MongoClient
from influxdb_client import InfluxDBClient

#Credenciales MongoDB
url_mongo = 'TU_URL_MONGO'                                                                                                   #URL de la base de datos MongoBD (No lleva puerto)
puerto_mongo = 'TU_PUERTO_MONGO'                                                                                                           #Puerto para establecer conexion con MongoDB
db_mongo = 'NOMBRE_DE_DB'                                                                                                             #Nombre de la base de datos en MongoDB     
collection_mongo = 'NOMBRE_DE_COLECCION'                                                                                          #Nombre de la coleccion en el db de MongoDB
user_mongo = 'USUARIO_MONGO'
pass_mongo = 'CONTRASEÑA_MONGO'

#Credenciales InfluxDB
url_influx = 'URL_INFLUX'                                                                                      #URL de la base de datos InfluxDB (Si lleva puerto)         
token_influx = 'TOKEN_INFLUX'                                                                              #Token para establecer conexion con InfluxDB
org_influx = 'ORGANIZACION_INFLUX'                                                                                                          #Org del bucket en InfluxDB
bucket1_influx = 'BUCKET1_INFLUX'                                                                                           #Bucket1 al que se quiere establecer conexion en Influx
bucket2_influx = 'BUCKET2_INFLUX'                                                                                                   #Bucket2 al que se quiere establecer conexion en Influx

#Funcion que devuelve el ultimo voto de confort en MongoDB
def obtener_monogo_ultimos():
    # Conectarse a la base de datos MongoDB
    client = MongoClient(url_mongo, puerto_mongo, username=user_mongo, password=pass_mongo)
    db = client[db_mongo]
    collection = db[collection_mongo]
    # Obtener los últimos datos de la colección
    ultimo_dato = collection.find().sort('_id', -1).limit(1)
    for dato in ultimo_dato:
        voto = dato['nivelComfort']
    # Cerrar la conexión a la base de datos
    client.close()
    return voto

#Funcion que retorna una lista de los ultimos valores almacenados en InfluxDB ([fecha_y_hora_actual, estado_AC, temperatura_seteada, temperatura_estacion, ocupancia])
def obtener_infllux_ultimos():
    # Configurar los parámetros de conexión
    url = url_influx
    token = token_influx
    org = org_influx
    bucket1 = bucket1_influx
    bucket2 = bucket2_influx
    # Conectarse al cliente de InfluxDB
    client = InfluxDBClient(url=url, token=token, org=org)
    fields_influx = ["estado","temp_set","temperatura"]
    valores=[]
    valores.append(datetime.now(pytz.timezone('America/Guayaquil')))
    for field in fields_influx:
        # Construir la consulta
        query = f'from(bucket:"{bucket1}") \
                |> range(start: -5d) \
                |> filter(fn: (r) => r._field == "{field}" and r._measurement == "stationChicaNoboa" and r.Ubicacion == "LST") \
                |> last()'
        # Ejecutar la consulta
        result = client.query_api().query(org=org, query=query)
        # Obtener los registros
        records = list(result)
        for record in records:
            for row in record.records:
                valor=row.values.get("_value")
                #Descomentar estas lineas si se quiere obtener las temperaturas desde la api
                #if field=="temp_set":
                #    valor=api.obtener_temperatura_actual()
                valores.append(valor)
    
    query = f'from(bucket:"{bucket2}") \
            |> range(start: -5d) \
            |> filter(fn: (r) => r._field == "count" and r._measurement == "person_count") \
            |> last()'
    # Ejecutar la consulta
    result = client.query_api().query(org=org, query=query)
    # Obtener los registros
    records = list(result)
    for record in records:
        for row in record.records:
            valor=row.values.get("_value")
            valores.append(valor)
    # Cerrar la conexión
    client.close()
    return valores

#Funcion que devuelve el promedio de los votos, en el tiempo especificado, de confort en MongoDB
def obtener_monogo_prom(funcion_timedelta: timedelta):
    client = MongoClient(url_mongo, puerto_mongo, username=user_mongo, password=pass_mongo)
    db = client[db_mongo]
    collection = db[collection_mongo]
    # Calcular la fecha y hora hace 30 minutos desde ahora
    fecha_y_hora_previa = datetime.now() - funcion_timedelta
    # Realizar la consulta para obtener los datos de los últimos 30 minutos
    datos_ultimos_tiempo_previo = collection.find({"fecha": {"$gte": fecha_y_hora_previa}})
    conteo_total = 0
    conteo_neutral = 0
    # Procesar los datos según sea necesario
    for dato in datos_ultimos_tiempo_previo:
        conteo_total += 1
        # Realizar acciones con cada registro
        if dato['nivelComfort'] == "neutral":
            conteo_neutral += 1
    # Cerrar la conexión a la base de datos
    if conteo_total > 0:
        promedio = conteo_neutral/conteo_total
    else:
        client.close()
        return None
    client.close()
    return promedio

#Funcion que retorna una lista con el promedio de los ultimos valores, en el tiempo especificado, almacenados en InfluxDB ([fecha_y_hora_actual, estado_AC, temperatura_seteada, temperatura_estacion, ocupancia])
#Otros peridos: "30s", "1h", "7d"
def obtener_influx_prom(periodo = "30m"):
    # Configurar los parámetros de conexión
    url = url_influx
    token = token_influx
    org = org_influx
    bucket1 = bucket1_influx
    bucket2 = bucket2_influx
    # Conectarse al cliente de InfluxDB
    client = InfluxDBClient(url=url, token=token, org=org)
    fields_influx = ["estado","temp_set","temperatura"]
    valores=[]
    valores.append(datetime.now(pytz.timezone('America/Guayaquil')))
    for field in fields_influx:
        # Construir la consulta
        query = f'from(bucket:"{bucket1}") \
            |> range(start: -5d) \
            |> filter(fn: (r) => r._field == "{field}" and r._measurement == "stationChicaNoboa" and r.Ubicacion == "LST") \
            |> aggregateWindow(every: {periodo}, fn: mean, createEmpty: false) \
            |> tail(n: 2)'
        # Ejecutar la consulta
        result = client.query_api().query(org=org, query=query)
        # Obtener los registros
        records = list(result)
        penultimo = records[0].records[0]
        if field == fields_influx[0]:
            if penultimo.values.get("_value") >= 0.5:
                valores.append(1.0)
            else: 
                valores.append(0.0)
        else:      
            valores.append(penultimo.values.get("_value"))

    
    query = f'from(bucket:"{bucket2}") \
            |> range(start: -5d) \
            |> filter(fn: (r) => r._field == "count" and r._measurement == "person_count") \
            |> filter(fn: (r) => r["_value"]>0) \
            |> aggregateWindow(every: {periodo}, fn: mean, createEmpty: false) \
            |> tail(n: 2)'
    # Ejecutar la consulta
    result = client.query_api().query(org=org, query=query)
    # Obtener los registros
    records = list(result)
    penultimo = records[0].records[0]

    diferencia = datetime.now(pytz.timezone('America/Guayaquil')) - penultimo.values.get("_time")
    if diferencia >= timedelta(hours=1):
        valores.append(0.0)
    else:
        valores.append(penultimo.values.get("_value")) 
    # Cerrar la conexión
    client.close()
    return valores

def ocupancia():
    client = InfluxDBClient(url=url_influx, token=token_influx, org=org_influx)
    query = f'from(bucket:"{bucket2_influx}") |> range(start: -30d) |> filter(fn: (r) => r._field == "count" and r._measurement == "person_count") |> filter(fn: (r) => r["_value"] >0 ) |> timeShift(duration: -5h) |> yield(name: "all")'
    valores=[]
    # Ejecutar la consulta
    result = client.query_api().query(org=org_influx, query=query)
    # Obtener los registros
    records = list(result)
    # Imprimir los datos obtenidos
    for record in records:
        for i, row in enumerate(record.records, start=1):  # Agregar un contador con inicio en 1
            valor=row.values.get("_value")
            tiempo=row.values.get("_time")
            valores.append({"position": i, "_time": tiempo, "_value": valor})
                # Cerrar la conexión
    client.close()
    return valores
 
def temperatura_interna():                                                                                         #Bucket1 al que se quiere establecer conexion en Influx
    valores =[]
    client = InfluxDBClient(url=url_influx, token=token_influx, org=org_influx)
    query = f'from(bucket:"{bucket1_influx}") |> range(start: -31d) |> filter(fn: (r) => r._field == "temperatura" and r._measurement == "stationChicaNoboa" and r.Ubicacion == "LST") |> timeShift(duration: -5h)|> yield(name: "all")'
    # Ejecutar la consulta
    result = client.query_api().query(org=org_influx, query=query)
    # Obtener los registros
    records = list(result)
    # Imprimir los datos obtenidos
    for record in records:
        for i, row in enumerate(record.records, start=1):  # Agregar un contador con inicio en 1
            valor=row.values.get("_value")
            tiempo=row.values.get("_time")
            valores.append({"position": i, "_time": tiempo, "_value": valor})
    # Cerrar la conexión
    client.close()
    return valores

def obtener_mongo():
    # Conectarse a la base de datos MongoDB
    client = MongoClient(url_mongo, puerto_mongo, username=user_mongo, password=pass_mongo)
    db = client[db_mongo]
    collection = db[collection_mongo]
    datos = collection.find({'estadoAC': 1}).sort('_id', -1)
    total_resultados = collection.count_documents({'estadoAC': 1})
    horas_mongo=[]
    # diccionario para agrupar los votos y temperaturas por hora y fecha
    votos_temperaturas_por_hora_fecha = {}
    # Iterar sobre los datos obtenidos
    for dato in datos:
        voto = dato['nivelComfort']
        fecha = dato['fecha']
        temp = dato['temperaturaAC']
        # Obtener solo la parte de la hora de la fecha
        hora = fecha.strftime('%H:00:00')
        # Agregar el voto y temperatura al diccionario de horas y fechas
        if hora not in votos_temperaturas_por_hora_fecha:
            votos_temperaturas_por_hora_fecha[hora] = []
        votos_temperaturas_por_hora_fecha[hora].append({'fecha': fecha, 'voto': voto, 'temp': temp})
    # Cerrar la conexión a la base de datos
    client.close()
    # Ordenar el diccionario por las horas
    votos_temperaturas_por_hora_fecha_ordenado = dict(sorted(votos_temperaturas_por_hora_fecha.items()))
    # return { 'resultados':votos_temperaturas_por_hora_fecha_ordenado,'total_resultados': total_resultados}
    # Imprimir los datos agrupados por hora y fecha
    for indice, (hora, datos_hora) in enumerate(votos_temperaturas_por_hora_fecha_ordenado.items(), start=1):
        #print(f"{indice}.  Hora: {hora} con sus fechas:")
        horas_mongo.append(hora)
        for dato_hora in datos_hora:
            fecha = dato_hora['fecha']
            voto = dato_hora['voto']
            temp = dato_hora['temp']
            #print(f"     Fecha: {fecha}, Voto: {voto}, Temp: {temp}")
    converted_list = [int(time[:2]) for time in horas_mongo]

    return converted_list, votos_temperaturas_por_hora_fecha_ordenado