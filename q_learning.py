import os
import csv
import time
import numpy as np
import bd_connector as bc
import api_connector as api
import obtener_recompensas as r
from datetime import datetime, timedelta

#Constantes
N_ACCIONES = 3
N_ESTADOS = 54
N_EPISODIOS = 20
N_PASOS = 30
#Acciones
UP = 2
DOWN = 1
MAINTAIN = 0
 
#Funcion que devuelve las acciones disponibles dependiendo de la temperatura actual del A/C
def get_actions(estado):
    ocupancia_list = ['POCA', 'MEDIA', 'MUCHA']
    temp_in_list = ['T1', 'T2']
    temp_ac_list = ['FRIO', 'NEUTRAL', 'CALOR']
    periodo_list = ['P1', 'P2', 'P3']

    acciones = []

    i = 0
    for a in periodo_list:
        for b in ocupancia_list:
            for c in temp_in_list:
                for d in temp_ac_list:
                    i += 1
                    if i == estado:
                        acciones.append(MAINTAIN)
                        if d != 'FRIO':
                            acciones.append(DOWN)
                        if d != 'CALOR':
                            acciones.append(UP)
    return acciones

#Funcion que devuelve el numero del estado segun la lista que recibe. La lista que recibe esta funcion debe tener el siguiente orden ([fecha_y_hora_actual, estado_AC, temperatura_seteada, temperatura_estacion, ocupancia])
def create_states(ocupancia, temperatura_interna, temperatura_ac, periodo):
    ocupancia_list = ['POCA', 'MEDIA', 'MUCHA']
    temp_in_list = ['T1', 'T2']
    temp_ac_list = ['FRIO', 'NEUTRAL', 'CALOR']
    periodo_list = ['P1', 'P2', 'P3']
    
    i = 0
    for a in periodo_list:
        for b in ocupancia_list:
            for c in temp_in_list:
                for d in temp_ac_list:
                    i += 1
                    if b == ocupancia and  c == temperatura_interna and d == temperatura_ac and a == periodo:
                        return i

#Funcion que devuelve el estado actual del LST. La lista que recibe esta funcion debe tener el siguiente orden ([fecha_y_hora_actual, estado_AC, temperatura_seteada, temperatura_estacion, ocupancia])
def get_state (lista_de_valores, recompensa):
    #Obtener periodo del dia
    if type(lista_de_valores[0]) == datetime:
        if lista_de_valores[0].hour >= 8 and lista_de_valores[0].hour < 11:
            periodo = 'P1'
        elif lista_de_valores[0].hour >= 11 and lista_de_valores[0].hour < 14:
            periodo = 'P2'
        elif lista_de_valores[0].hour >= 14 and lista_de_valores[0].hour <= 18:
            periodo = 'P3'
    else:
        if lista_de_valores[0] >= 8 and lista_de_valores[0] < 11:
            periodo = 'P1'
        elif lista_de_valores[0] >= 11 and lista_de_valores[0] < 14:
            periodo = 'P2'
        elif lista_de_valores[0] >= 14 and lista_de_valores[0] <= 18:
            periodo = 'P3'

    #Obtener temperatura A/C
    if lista_de_valores[2] >= 16.0 and lista_de_valores[2] < 20.0:
        temperatura_ac = 'FRIO'
    elif lista_de_valores[2] >= 20.0 and lista_de_valores[2] < 23.0:
        temperatura_ac = 'NEUTRAL'
    elif lista_de_valores[2] >= 23.0 and lista_de_valores[2] <= 24.0:
        temperatura_ac = 'CALOR'

    #Obtener temperatura de la estacion
    if lista_de_valores[3] >= 21.0 and lista_de_valores[3] < 24.0:
        temperatura_interna = 'T1'
    elif lista_de_valores[3] >= 24.0 and lista_de_valores[3] <= 27.0:
        temperatura_interna = 'T2'
    
    #Obtener ocupancia
    if lista_de_valores[4] >= 0.0 and lista_de_valores[4] < 7.0:
        ocupancia = 'POCA'
    elif lista_de_valores[4] >= 7.0 and lista_de_valores[4] < 14.0:
        ocupancia = 'MEDIA'
    elif lista_de_valores[4] >= 14.0 and lista_de_valores[4] <= 20.0:
        ocupancia = 'MUCHA'

    #Obtener estado 
    return (create_states(ocupancia, temperatura_interna, temperatura_ac, periodo), recompensa)

def get_next_state(accion: int, temperatura):
    #accionar
    #obtener estado
    if accion == 0:
        #temperatura = api.obtener_temperatura_actual()
        return get_state(bc.obtener_infllux_ultimos(), None), temperatura
    elif accion == 1:
        #temperatura = api.obtener_temperatura_actual()
        api.cambiar_temperatura_actual(temperatura-2)
        return get_state(bc.obtener_infllux_ultimos(), None), temperatura-2
    elif accion == 2:
        #temperatura = api.obtener_temperatura_actual()
        api.cambiar_temperatura_actual(temperatura+2)
        return get_state(bc.obtener_infllux_ultimos(), None), temperatura+2

def get_reward(estado: int):
    promedio = bc.obtener_monogo_prom(timedelta(minutes=60))
    if promedio is not None:
        #obtener recompensa online
        if promedio >= 0.5:
            return 1
        else:
            return 0
    else:
        #obtener recompensa offline
        diccionario = r.estados_recompensas()
        recompensa = diccionario[estado]
        return recompensa
    
def get_index(accion, lista_acciones):
    if len(lista_acciones) != N_ACCIONES:
        if accion==2:
            return 1
        else:
            return accion
    else:
        return accion

def save_tables(tabla_q, tabla_prob, tabla_pi, nombre_archivo: str):
    with open(nombre_archivo, 'w', newline='') as archivo_csv:
        #Crear el escritor CSV
        escritor_csv = csv.writer(archivo_csv)

        #Guardar Tabla q
        escritor_csv.writerow(["Tabla_q", datetime.now()])
        for i in tabla_q.keys():
            num_datos=len(tabla_q[i])
            fila=[]
            fila.append(i)
            for dato in range(0,num_datos):
                fila.append(tabla_q[i][dato])
            escritor_csv.writerow(fila)
        
        #Guardar Tabla de probabilidades
        escritor_csv.writerow(["Tabla_prob", datetime.now()])
        for i in tabla_prob.keys():
            num_datos=len(tabla_prob[i])
            fila=[]
            fila.append(i)
            for dato in range(0,num_datos):
                fila.append(tabla_prob[i][dato])
            escritor_csv.writerow(fila)

        #Guardar tabla de politicas
        escritor_csv.writerow(["Tabla_pi", datetime.now()])
        for i in range(0, len(tabla_pi)):
            fila=[int(i)+1,int(tabla_pi[i])]
            escritor_csv.writerow(fila)

def save_all_tables(tabla_q, tabla_prob, tabla_pi, nombre_archivo: str):
    #Abrir el archivo en modo de escritura
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        #Crear el escritor CSV
        escritor_csv = csv.writer(archivo_csv)

        #Guardar Tabla q
        escritor_csv.writerow(["Tabla_q", datetime.now()])
        for i in tabla_q.keys():
            num_datos=len(tabla_q[i])
            fila=[]
            fila.append(i)
            for dato in range(0,num_datos):
                fila.append(tabla_q[i][dato])
            escritor_csv.writerow(fila)
        
        #Guardar Tabla de probabilidades
        escritor_csv.writerow(["Tabla_prob", datetime.now()])
        for i in tabla_prob.keys():
            num_datos=len(tabla_prob[i])
            fila=[]
            fila.append(i)
            for dato in range(0,num_datos):
                fila.append(tabla_prob[i][dato])
            escritor_csv.writerow(fila)

        #Guardar tabla de politicas
        escritor_csv.writerow(["Tabla_pi", datetime.now()])
        for i in range(0, len(tabla_pi)):
            fila=[int(i)+1,int(tabla_pi[i])]
            escritor_csv.writerow(fila)

#Se retornan las tablas en el siguiente orden (tabla_q, tabla_prob, tabla_pi)
def import_tables(nombre_archivo: str):
    diccionarios = []
    bloque_actual = None

    with open(nombre_archivo, 'r', newline='') as archivo_csv:
        lector_csv = csv.reader(archivo_csv)
        strings_a_buscar=["Tabla_q", "Tabla_prob", "Tabla_pi"]

        nombre_tabla = ""
        for fila in lector_csv:
            if (any(fila[0] == s for s in strings_a_buscar)):
                nombre_tabla=fila[0]
                bloque_actual = {}
                if nombre_tabla == "Tabla_pi":
                    bloque_actual = []
                diccionarios.append(bloque_actual)
            else:
                clave = fila[0]
                clave_int = int(clave)
                if nombre_tabla == "Tabla_pi":
                    valor_int = int(fila[1])
                    bloque_actual.append(valor_int)
                else:
                    lista=[]
                    for i in range(1, len(fila)):
                        valor_float = float(fila[i])
                        lista.append(valor_float)
                    bloque_actual[clave_int] = lista
    return diccionarios

#Crear directorio para archivos csv
def crear_directorio(nombre_directorio):
    directorio_script = os.path.dirname(__file__)
    ruta_completa = os.path.join(directorio_script, nombre_directorio)
    
    if os.path.exists(ruta_completa):
        return 0
    else:
        try:
            os.makedirs(ruta_completa, exist_ok=True)
            return 0
        except OSError as error:
            return 1

def qlearning(alpha: float, gamma: float, epsilon: float, tabla_probabilidades = None, tabla_q = None, tabla_politicas = None):
    now = datetime.now()
    day_of_week = now.weekday()
    hour = now.hour

    if 0 <= day_of_week <= 4:
        #Inicializar tabla de probabilidades estocasticas para cada accion
        if tabla_probabilidades is None:
            pi_q = {k:[1/len(get_actions(k)) for a in range(len(get_actions(k)))] for k in range(1,55)}
        else:
            pi_q = tabla_probabilidades
        
        #inicializar diccionario con valores q para todos los estados, diccionario se inicializa en 0
        if tabla_q is None:
            q_table = {k:[0 for a in range(len(get_actions(k)))] for k in range(1, 55)}
        else:
            q_table = tabla_q

        #Inicializar tabla de politicas, se inicializa en 0
        if tabla_politicas is None:
            pi = np.zeros((N_ESTADOS, 1), dtype=np.int32)
        else:
            pi = tabla_politicas

        temp_actual = api.obtener_temperatura_actual()
        temp_anterior = temp_actual
        temp_q = temp_anterior

        for epi in range(N_EPISODIOS):
            for t in range(N_PASOS):
                hour = datetime.now().hour
                if 8 <= hour < 18:
                    if api.obtener_estado_actual() == 1:
                        temp_actual = api.obtener_temperatura_actual()
                        if (temp_q != temp_actual):
                            temp_q = temp_actual 
                            temp_anterior = temp_actual
                            time.sleep(60*30)
                        else:
                            #Esto se realiza cuando el usuario no ha hecho nada
                            time.sleep(60) #Tiempo en segundos. (1 minuto)

                            #Obtener estado del LST (ultimos valores registrados en influx)
                            lista = bc.obtener_infllux_ultimos()
                            estadoActualLST,_ = get_state(lista, None)
                            #Obtener acciones con la temperatura acutual del A/C del LST (API)
                            acciones = get_actions(estadoActualLST)
                            #Obtener accion random con la lista de acciones obtenidos previamente
                            prob = pi_q[estadoActualLST]
                            print(prob)
                            accion = np.random.choice(acciones, p=prob)
                            #Obtenemos el siguiente estado
                            nuevo_estado_LST, temp_q = get_next_state(accion, temp_actual)
                            #obtenemos recompensa
                            recompensa = get_reward(nuevo_estado_LST[0])

                            #Actualizar tabla Q
                            p1=q_table[estadoActualLST][get_index(accion, acciones)]
                            p2=np.max(q_table[nuevo_estado_LST[0]])
                            q_table[estadoActualLST][get_index(accion, acciones)] =  p1+ alpha * (recompensa + gamma*p2 - p1)

                            #Determinar accion optima
                            accion_optima = q_table[estadoActualLST].index(max(q_table[estadoActualLST]))
                            indice_accion_optima = get_index(accion_optima, acciones)

                            #Guardar accion optima en tabla pi
                            pi[estadoActualLST-1] = accion_optima

                            for i in acciones:
                                indice = acciones.index(i)
                                if indice == indice_accion_optima:
                                    pi_q[estadoActualLST][indice] = 1 - epsilon + epsilon/len(acciones)
                                else:
                                    pi_q[estadoActualLST][indice] = epsilon/len(acciones)
                            temp_anterior = temp_actual                              
                    else:
                        if api.obtener_estado_actual() == 1:
                            temp_q = api.obtener_temperatura_actual()
                        pass
                else:
                    if hour>=18:
                        calculo = 32-hour
                    else:
                        calculo = 8-hour
                    time.sleep(60*60*calculo) #Tiempo en segundos. (14-1 hora)
            if api.obtener_estado_actual() == 1 and 8 <= hour < 18: #REVISAR ESTE IF
                save_tables(q_table, pi_q, pi, "./csv_files/last_tables.csv")
                save_all_tables(q_table, pi_q, pi, "./csv_files/hitorical_record.csv")
        return q_table, pi_q, pi

    else:
        time.sleep(60*60*24) #Tiempo en segundos. (1 dia)
        return tabla_q, tabla_probabilidades, tabla_politicas

                    
def main():
    directorio_existe = crear_directorio("csv_files")
    if directorio_existe == 0:
        try:
            lista = import_tables("./csv_files/last_tables.csv") #(tabla_q, tabla_prob, tabla_pi)
            tabla_q = lista[0]
            tabla_prob = lista[1]
            tabla_pi = lista[2]
        except FileNotFoundError:
            tabla_q = tabla_prob = tabla_pi = None
        while True:
            #El valor alpha determina que tan rapido se obtienen los valores q. El valor gamma determina la ponderacion de las recompensas futuras con respecto a la recompensa inmediata \
            #El valor epsilon determina la probabilidad de explorar. 
            tabla_q, tabla_prob, tabla_pi = qlearning(alpha=0.3, gamma=0.9, epsilon=0.9, tabla_probabilidades=tabla_prob, tabla_q=tabla_q, tabla_politicas=tabla_pi)
    else:
        return
        
if __name__ == "__main__":
    main()