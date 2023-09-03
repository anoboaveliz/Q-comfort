from collections import OrderedDict
from bd_connector import ocupancia, temperatura_interna, obtener_mongo
from q_learning import get_state


# funcion que recoge las horas que está registrados los votos y trae las temperaturas a esas horas
def temperaturasI_por_hora():
    # Obtener las horas y fechas de los votos y temperaturas de MongoDB
    horas_mongo,_ = obtener_mongo()
    # Obtener las temperaturas internas de InfluxDB
    datos_temperatura = temperatura_interna()
    temperaturas_interna_xhorasdemongo={}
    # Iterar sobre los datos de temperatura
    for dato in datos_temperatura:
        # Obtener solo la parte de la hora de la fecha
        hora = dato['_time'].hour
        # Verificar si la hora está en las horas de MongoDB
        if hora in horas_mongo:
            # Agregar la temperatura al diccionario
            if hora not in temperaturas_interna_xhorasdemongo:
                temperaturas_interna_xhorasdemongo[hora] = []
            #para verificar que si trae correctamente por hora
            #temperaturas_interna_xhorasdemongo[hora].append(dato)
            temperaturas_interna_xhorasdemongo[hora].append(dato["_value"])
    temperaturas_interna_xhorasdemongo = OrderedDict(sorted(temperaturas_interna_xhorasdemongo.items()))

    return datos_temperatura,temperaturas_interna_xhorasdemongo

def ocupancia_por_hora():
    # Obtener las horas y fechas de los votos y temperaturas de MongoDB
    horas_mongo,_ = obtener_mongo()
    # Obtener las temperaturas internas de InfluxDB
    datos_ocupante = ocupancia()
    ocupantes_xhorasdemongo={}
    # Iterar sobre los datos de temperatura
    for dato in datos_ocupante:
        # Obtener solo la parte de la hora de la fecha
        hora = dato['_time'].hour
        # Verificar si la hora está en las horas de MongoDB
        if hora in horas_mongo:
            # Agregar la temperatura al diccionario
            if hora not in ocupantes_xhorasdemongo:
                ocupantes_xhorasdemongo[hora] = []
            #para verificar que si trae correctamente por hora
            #temperaturas_interna_xhorasdemongo[hora].append(dato)
            ocupantes_xhorasdemongo[hora].append(dato["_value"])
    ocupantes_xhorasdemongo = OrderedDict(sorted(ocupantes_xhorasdemongo.items()))

    return datos_ocupante,ocupantes_xhorasdemongo

def calcular_promedio_temperaturas():
    # Obtener las temperaturas por hora
    _,temperaturas_por_hora = temperaturasI_por_hora()
    promedios_por_hora = {}
    # Iterar sobre las temperaturas por hora
    for hora, temperaturas in temperaturas_por_hora.items():
        # Calcular el promedio de las temperaturas
        promedio = sum(temperaturas) / len(temperaturas)
        # Agregar el promedio al diccionario
        promedios_por_hora[hora] = promedio
    # Ordenar el diccionario por las claves (horas)
    promedios_por_hora_ordenado = dict(sorted(promedios_por_hora.items()))

    return promedios_por_hora_ordenado

def calcular_promedio_ocupantes():
    # Obtener las temperaturas por hora
    _,ocupantes_por_hora = ocupancia_por_hora()
    promedios_por_hora = {}
    # Iterar sobre las temperaturas por hora
    for hora, ocupantes in ocupantes_por_hora.items():
        # Calcular el promedio de las temperaturas
        promedio = sum(ocupantes) / len(ocupantes)
        # Agregar el promedio al diccionario
        promedios_por_hora[hora] = promedio
    # Ordenar el diccionario por las claves (horas)
    promedios_por_hora_ordenado = dict(sorted(promedios_por_hora.items()))

    return promedios_por_hora_ordenado

def calcular_recompensas_hora_votos():
    # Obtener los datos de la funcion de mongo
    _, votos_temperaturas_por_hora_fecha = obtener_mongo()
    conteos = {}
    # Iterar sobre los datos
    for hora, datos_hora in votos_temperaturas_por_hora_fecha.items():
        for dato_hora in datos_hora:
            voto = dato_hora['voto']
            temp = dato_hora['temp']
            # Si la hora y temperatura no están en el diccionario, agregarlos
            if (hora, temp) not in conteos:
                conteos[(hora, temp)] = {'frio': 0, 'neutral': 0, 'calor': 0}
            # Incrementar el conteo para el tipo de voto
            conteos[(hora, temp)][voto] += 1
    # Calcular los porcentajes
    recompensas_xvotos = {}
    for (hora, temp), conteo in conteos.items():
        total = sum(conteo.values())
        max_voto = max(conteo, key=conteo.get)
        recompensas_xvotos[(hora, temp)] = 1 if max_voto == 'neutral' or conteo['neutral'] >= total * 0.5 else 0

    return recompensas_xvotos

def compacion_visual_calculo_porcentajeshxv():
    # Obtener los datos de MongoDB
    _, votos_temperaturas_por_hora_fecha = obtener_mongo()
    conteos = {}
    # Iterar sobre los datos
    for hora, datos_hora in votos_temperaturas_por_hora_fecha.items():
        for dato_hora in datos_hora:
            voto = dato_hora['voto']
            temp = dato_hora['temp']
            # Si la hora y temperatura no están en el diccionario, agregarlos
            if (hora, temp) not in conteos:
                conteos[(hora, temp)] = {'frio': 0, 'neutral': 0, 'calor': 0}
            # Incrementar el conteo para el tipo de voto
            conteos[(hora, temp)][voto] += 1
    # Calcular los porcentajes
    porcentajes_visuales = {}
    for (hora, temp), conteo in conteos.items():
        total = sum(conteo.values())
        porcentajes_visuales[(hora, temp)] = {voto: (count / total) * 100 for voto, count in conteo.items()}

    return porcentajes_visuales

def combinar_datos():
    # Obtener los promedios de temperaturas y ocupantes
    promedios_temperaturas = calcular_promedio_temperaturas()
    promedios_ocupantes = calcular_promedio_ocupantes()
    votos = calcular_recompensas_hora_votos()

    # Crear un diccionario para almacenar los datos combinados
    datos_combinados = {}

    # Iterar sobre los votos
    for (hora, temp), voto in votos.items():
        # Convertir la hora a un entero
        hora = int(hora.split(':')[0])

        # Si la hora no está en el diccionario, agregarla
        if hora not in datos_combinados:
            datos_combinados[hora] = {}

        # Si la hora existe en los promedios, agregar los datos
        if hora in promedios_temperaturas and hora in promedios_ocupantes:
            datos_combinados[hora][temp] = [promedios_temperaturas[hora], promedios_ocupantes[hora], voto]

    return datos_combinados

#funcion de recolecta todos los estados que obtengo
def estados_recompensas():
    datos_combinados = combinar_datos()
    estados_recompensas = {i: [0] for i in range(1, 55)}

    # Iterar sobre todos los elementos del diccionario
    for hora, datos_hora in datos_combinados.items():
        for temperaturaAC, datos_temp in datos_hora.items():
            # Desempaquetar los datos
            temperaturaI, ocupancia, recompensa = datos_temp

            # Crear la lista para pasar a get_state
            lista = [hora,1, temperaturaAC, temperaturaI, ocupancia]

            # Llamar a get_state
            estado, _ = get_state(lista,recompensa)

            # Si el estado ya está en el diccionario, agregar la recompensa a la lista de recompensas
            if estado in estados_recompensas:
                estados_recompensas[estado].append(recompensa)
            # Si el estado no está en el diccionario, crear una nueva lista para las recompensas
            else:
                estados_recompensas[estado] = [recompensa]
    diccionario_update=filtro_estados_rep(estados_recompensas)

    return diccionario_update

#filtrar estados repetidos
def filtro_estados_rep(dict_estados_recompensas):
    # Crear un nuevo diccionario para guardar los estados y las recompensas más frecuentes
    nuevos_estados_recompensas = {}
    # Iterar sobre el diccionario de estados y recompensas
    for estado, recompensas in dict_estados_recompensas.items():
        # Contar el número de veces que cada recompensa aparece
        conteo_recompensas = {recompensa: recompensas.count(recompensa) for recompensa in recompensas}
        # Encontrar la recompensa más frecuente
        recompensa_mas_frecuente = max(conteo_recompensas, key=conteo_recompensas.get)
        # Si hay un empate, elegir la recompensa 1
        if conteo_recompensas[recompensa_mas_frecuente] == conteo_recompensas.get(1, 0):
            recompensa_mas_frecuente = 1
        # Agregar el estado y la recompensa más frecuente al nuevo diccionario
        nuevos_estados_recompensas[estado] = recompensa_mas_frecuente
    
    # Devolver el nuevo diccionario
    return nuevos_estados_recompensas