import requests

api_url = "TU_URL"

def obtener_temperatura_actual():
    resp = requests.get(api_url+'temp').json()
    return int(resp['Message'])

def obtener_estado_actual():
    resp = requests.get(api_url+'estado').json()
    return int(resp['Message'])

def cambiar_temperatura_actual(nueva_temperatura):
    if nueva_temperatura >= 16 and nueva_temperatura <=24:
        requests.patch(url = api_url+'temp/'+str(nueva_temperatura))

def cambiar_estado_actual(nuevo_estado):
    if nuevo_estado == 0 or nuevo_estado == 1:
        requests.patch(url = api_url+'estado/'+str(nuevo_estado))