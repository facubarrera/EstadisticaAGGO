#########################################
#########################################

#Programa para importar informacion desde los reportes de IVS sobre la performance de las estaciones involucradas en la sesion.(Barrera F., 2024)

#Se crea una tabla con las columnas: 'anio'[año en que se llevo a cabo la sesión],
#                                    'mes' [mes en que se llevo a cabo la sesión]
# 									 'dia' [día en que se llevo a cabo la sesión]
#									 'estaciones_participantes' [lista de estaciones participantes en la sesión]
#									 'AGGOpresente' [variable booleana. True indica que AGGO esta presente la sesión]
#                                    'scheduled' [lista de cantidad de observaciones programadas para cada estación de la sesión. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#									 'recoverable'[lista de cantidad de observaciones usables para cada estación de la sesión. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#									 'used' [lista de cantidad de observaciones usadas para cada estación durante el análisis de la sesión. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#									 'scheduledAGGO' [lista de cantidad de observaciones programadas para cada línea de base entre AGGO y las estaciones de la sesión. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#									 'recoverableAGGO' [lista de cantidad de observaciones usables por cada la línea de base realizada entre AGGO y las estaciones de la sesión. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#                                    'usedAGGO' [lista de cantidad de observaciones usadas por la línea de base entre AGGO y cada estación de la sesión durante su análisis. Se ordena según el orden de estaciones presentadas en la columna 'estaciones_participantes']
#									 'problemasAGGO' [Variable booleana donde True indica que AGGO tuvo problemas durante esa sesión]
#									 'problemasAGGO_desc' [Descripción de los problemas que tuvo AGGO durante la sesión]
#									 'MJD' [Modified Julian Date en el que la sesión fue llevada a cabo]

##################################################################################
import requests
import pandas as pd
import subprocess
import os
from glob import glob
import numpy as np
import csv
import numpy as np

def descargar(lista,anio):
    print('Descargando')
    sesiones_sin_ivs=[]
    for sesion in lista:
        path='https://ivscc.gsfc.nasa.gov/sessions/'+anio+'/'+sesion #pedimos la información de la página donde estan los reportes
        x=requests.get(path)
        w=x.content.decode('utf-8').split('"') # en las siguientes lineas buscamos que reportes existen en esa página web.
        indices = [i for i, e in enumerate(w) if e[0:5]=='https']
        result = [w[i] for i in indices]
        indices=[i for i,e in enumerate(result) if e[-4:]=='.txt']
        txt=[result[i] for i in indices]
        indices=[i for i,e in enumerate(txt) if ('IVS-analysis-report' in e) or ('ivs-analysis-report' in e)]
        txt=[txt[i] for i in indices]
        df=pd.DataFrame({'url':txt,'fecha':[i[-17:-4] for i in txt]}) # hacemos una tablas con los url de los reportes encontrados para la sesión y su fecha.
        try: # intentamos obtener la fecha en el que los reportes fueron creados
            df['año']=df.fecha.apply(lambda row: int(row[0:4]))
            df['mes']=df.fecha.apply(lambda row: int(row[4:6]))
            df['dia']=df.fecha.apply(lambda row: int(row[6:8]))
            df['hora']=df.fecha.apply(lambda row: int(row[9:11]))
            df['minuto']=df.fecha.apply(lambda row: int(row[11:13]))
            df=df.sort_values(['año','mes','dia','hora','minuto'],ascending=False).reset_index(drop=True) #ordenamos la tabla para que me ponga el reporte mas actual primero
        except ValueError: # si no se puede encontrar la fecha dejamos la tabla como esta
            df=df
        folder='./analysis_report'+anio
        if not os.path.isdir(folder):#creo la carpeta donde se descargarán los reportes
            os.mkdir(folder)
        if (df.shape[0]!=0) and (not(os.path.isfile(folder+'/IVS-analysis-report-' + sesion+'.txt'))):# descargo el más actual si es que existe un reporte y ya no esta descargado
            print(folder+'/IVS-analysis-report-' + sesion+'.txt')
            shell_command='curl -c usr_cookies.dat -b usr_cookies.dat --netrc-file ./.netrc -L  "'+df.loc[0,'url'] +'" >'+ folder+'/IVS-analysis-report-' + sesion+'.txt'
            result=subprocess.run(shell_command, shell=True, capture_output=True, text=True)
            print('Se descargo','/IVS-analysis-report-',sesion,'.txt')
        else:		
            sesiones_sin_ivs.append(sesion)#creo una lista de las sesiones que no tienen reportes
    nombre='./sesiones_sin_ivs'+anio+'.txt'
    h=open(nombre,'w')
    for i in sesiones_sin_ivs:
        h.write(i + '\n')
    h.close()

def listar_sesiones(anio):
    print('Listando sesiones')
    v=requests.get('https://ivscc.gsfc.nasa.gov/sessions/'+ anio +'/') # Le pido que entre a la página web que se indica donde se listan las sesiones llevadas a cabo en cada año.
    separado=v.content.decode('utf-8').split('"') # al contenido que tomamos del request lo escribimos en formato utf-8 y luego  buscamos las sesiones del tipo r1 y r4, y las listamos.
    indices=[i for i,e in enumerate(separado) if ('/sessions/'+anio+'/r1' in e)or ('/sessions/'+anio+'/r4' in e)  ]
    lista_sesiones=[separado[i].split('/')[-2] for i in indices]
    return lista_sesiones

def cargar_performance(tabla,lista,anio):
    print('Cargando performance')
    path='./analysis_report'+anio+'/IVS-analysis-report-r*'
    desc=glob(path)#listo que reportes tengo descargados
    #predefino las listas que voy a cargar a la tabla.
    tabla['anio']=[[]]*len(lista)
    tabla['mes']=[[]]*len(lista)
    tabla['dia']=[[]]*len(lista)
    tabla['estaciones_participantes']=[[]]*len(lista)
    tabla['AGGOpresente']=[False]*len(lista)
    tabla['scheduled']=[[]]*len(lista)
    tabla['recoverable']=[[]]*len(lista)
    tabla['used']=[[]]*len(lista)
    tabla['scheduledAGGO']=[[]]*len(lista)
    tabla.set_index(pd.Index(lista),inplace=True)
    lestaciones_participantes=[]
    lscheduled=[]
    lscheduledAGGO=[]
    lrecoverable=[]
    lrecoverableAGGO=[]
    lused=[]
    lusedAGGO=[]
    lanio=[]
    lmes=[]
    ldia=[]
    for j in lista: # cargo la información de cada sesion de la lista
        path_sesion= './analysis_report'+anio+'\\IVS-analysis-report-'+j+'.txt'
        if path_sesion in desc:
            lista_estaciones,lista_scheduled,lista_recoverable,lista_used,presente=leer_estaciones_participantes(path_sesion)#leo que estaciones participan de la sesion y su performance
            lestaciones_participantes.append(lista_estaciones)
            lscheduled.append(lista_scheduled)
            lrecoverable.append(lista_recoverable)
            lused.append(lista_used)	
            tabla.loc[j,'AGGOpresente']=presente
            anio2,mes,dia=leer_fecha(path_sesion,anio)
            lanio.append(anio2)
            lmes.append(mes)
            ldia.append(dia)
            if presente: #si AGGO esta presente en la sesion intento leer como fue su performance en la linea de base con las demás estaciones
                lista_scheduledAGGO,lista_recoverableAGGO,lista_usedAGGO=leer_lineasbase_AGGO(path_sesion,lista_estaciones)#leo la información sobre las líneas de base
                lscheduledAGGO.append(lista_scheduledAGGO)
                lrecoverableAGGO.append(lista_scheduledAGGO)
                lusedAGGO.append(lista_scheduledAGGO)
            else:# si no esta AGGO no agrego información
                lscheduledAGGO.append(['NOT']*len(lista_estaciones))
                lrecoverableAGGO.append(['NOT']*len(lista_estaciones))			
                lusedAGGO.append(['NOT']*len(lista_estaciones))
        else:# si no tengo el reporte no cargo información
            tabla.loc[j,'AGGOpresente']=False
            lestaciones_participantes.append([])
            lscheduled.append([])
            lrecoverable.append([])
            lused.append([])
            lscheduledAGGO.append([])
            lrecoverableAGGO.append([])
            lusedAGGO.append([])
            lanio.append([])
            lmes.append([])
            ldia.append([])
    tabla['anio']=lanio
    tabla['mes']=lmes
    tabla['dia']=ldia
    tabla['estaciones_participantes']=lestaciones_participantes
    tabla['scheduled']=lscheduled
    tabla['recoverable']=lrecoverable
    tabla['used']=lused
    tabla['scheduledAGGO']=lscheduledAGGO
    tabla['recoverableAGGO']=lrecoverableAGGO
    tabla['usedAGGO']=lusedAGGO
    return tabla

def leer_estaciones_participantes(file):
    print('Leyendo estaciones participantes')
    contenido=open(file,'r')
    lineas=contenido.readlines()
    numero_de_linea_inicio=[i+7 for i, e in enumerate(lineas) if 'Station Performance' in e ][0] #busco el indice de la linea donde se encuentra el inicio de 'Sation Performance'
    numero_de_linea_final=[i-1 for i, e in enumerate(lineas) if 'Station Total' in e ][0]# Aca busco el final
    lista_estaciones=[]
    lista_scheduled=[]
    lista_recoverable=[]
    lista_used=[]
    for j in lineas[numero_de_linea_inicio:numero_de_linea_final]:
        estacion=j.split(' ')
        estacion_filt=['NOT CORR' if i=='CORR' else i for i in estacion if (i!='') & (i!='NOT')]
        estacion_filt=['NOT USED' if i=='USED' else i for i in estacion_filt]
        lista_estaciones.append(estacion_filt[0]) #extraigo la informacion
        lista_scheduled.append(estacion_filt[1])
        lista_recoverable.append(estacion_filt[2])
        lista_used.append(estacion_filt[3])
    presente= 'AGGO' in lista_estaciones #busco si AGGO esta en la lista de estaciones
    contenido.close()
    return lista_estaciones,lista_scheduled,lista_recoverable,lista_used,presente

def leer_lineasbase_AGGO(file,lista_estaciones):
    contenido=open(file,'r')
    lineas=contenido.readlines()
    lista_scheduled=[]
    lista_recoverable=[]
    lista_used=[]
    for estacion_linea in lista_estaciones:
        if estacion_linea=='AGGO':# si busco información de la línea AGGO-AGGO no voy a encontrar, pongo 0 y salto a buscar la linea de base con la siguiente estación
            lista_scheduled.append('0')
            lista_recoverable.append('0')
            lista_used.append('0')
            continue
        linea_de_base1='AGGO-' + estacion_linea
        linea_de_base2=estacion_linea + '-AGGO'
        numero_de_linea=[i for i, e in enumerate(lineas) if (linea_de_base1 in e) or (linea_de_base2 in e)][0] #busco donde esta la información de la línea de base
        estacion=lineas[numero_de_linea].split(' ')
        estacion_filt=['NOT CORR' if i=='CORR' else i for i in estacion if (i!='') & (i!='NOT')]
        estacion_filt=['NOT DATA' if i=='DATA' else i for i in estacion_filt]
        lista_scheduled.append(estacion_filt[1]) #extraigo la información
        lista_recoverable.append(estacion_filt[2])
        lista_used.append(estacion_filt[3])
    contenido.close()
    return lista_scheduled,lista_recoverable,lista_used
	
def leer_fecha(file,anio):
    contenido=open(file,'r')
    linea=contenido.readlines()[0]
    ind=linea.index('(')
    if int(anio)<2023:
        anio='20'+linea[ind+2:ind+4]
        mes_string=linea[ind+4:ind+7]
        mes=convertir_mes(mes_string)
        dia=linea[ind+7:ind+9]
        dia=dia[0] if dia[1]=='-' else dia 
    else:
        anio=linea[ind+1:ind+5]
        mes=linea[ind+5:ind+7]
        dia=linea[ind+7:ind+9]
        dia=dia[0] if dia[1]=='-' else dia 
    contenido.close()
    return anio,mes,dia
	
def convertir_mes(mes_string):
    dic={'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06','JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
    mes=dic[mes_string]
    return mes
	
def cargar_problemas(tabla,lista,anio):
    path='./analysis_report'+anio+'/IVS-analysis-report-r*'
    desc=glob(path)
    tabla['problemasAGGO']=[False]*len(lista)
    tabla['problemasAGGO_desc']=['']*len(lista)
    tabla.set_index(pd.Index(lista),inplace=True)
    for j in lista:
        path_sesion= './analysis_report'+anio+'\\IVS-analysis-report-'+j+'.txt'
        if (path_sesion in desc) and tabla.AGGOpresente[j]:# si AGGO esta presente busco si tuvo problema
            tof=aggo_problema(path_sesion)#busco si aggo tuvo problema
            tabla.loc[j,'problemasAGGO']=tof
            if tof:
                descripcion=leer_descripcion(path_sesion,tabla.estaciones_participantes[j])#cargo la descripción del problema
                tabla.loc[j,'problemasAGGO_desc']=descripcion
        else:
            tabla.loc[j,'problemasAGGO']=False
    return tabla
	
def aggo_problema(file):
    contenido=open(file,'r')
    lineas=contenido.readlines()
    numero_de_linea_inicio=[i for i, e in enumerate(lineas) if 'Problems' in e ][0] # busco la sección de problemas
    numero_de_linea_final=[i-1 for i, e in enumerate(lineas) if 'Parameterization comments' in e ][0]
    todas_lineas=''
    for i in lineas[numero_de_linea_inicio:numero_de_linea_final]:
        todas_lineas=todas_lineas+i
    resultado= 'AGGO' in todas_lineas #busco si dice AGGO en algun lugar de la sección
    return resultado
	
def leer_descripcion(file,estaciones):
    contenido=open(file,'r')
    lineas=contenido.readlines()
    numero_de_linea_inicio=[i for i, e in enumerate(lineas) if 'Problems' in e ][0]# busco la sección de problemas
    numero_de_linea_final=[i-1 for i, e in enumerate(lineas) if 'Parameterization comments' in e ][0]
    numero_de_linea_AGGOd=[i for i, e in enumerate(lineas[numero_de_linea_inicio:numero_de_linea_final]) if 'AGGO' in e ][0]
    numero_de_linea_AGGOi=numero_de_linea_inicio+numero_de_linea_AGGOd # busco en la sección de problemas, que parte me describe el problema de AGGO
    numero_de_linea_AGGOf=[ i+1 for i, e in enumerate(lineas[numero_de_linea_AGGOi+1:numero_de_linea_final]) if esta_estacion(e,estaciones) ]# el número de línea final es donde empieza la descripción de otra estación
    if numero_de_linea_AGGOf==[]:
        numero_de_linea_AGGOf=numero_de_linea_final
    else:
        numero_de_linea_AGGOf=numero_de_linea_AGGOi+numero_de_linea_AGGOf[0]

    descripcion=lineas[numero_de_linea_AGGOi].replace('Problems: ','') # cargo la descripción

    for i in lineas[numero_de_linea_AGGOi+1:numero_de_linea_AGGOf]:
        descripcion=descripcion+i
    descripcion=descripcion.replace('(Ag) ','')
    return descripcion

def esta_estacion(e,estaciones):# busco si esta la estción en la lista
    res=False
    for i in estaciones:
         if i in e:
             res=True
    return res


def tabla_reports(anios): #Creamos tabla
    tabla=pd.DataFrame({})
    for anio in anios:
        print('Armando tabla para',anio)
        tabla_anio=pd.DataFrame({})	
        lista=listar_sesiones(anio) #Busco las sesiones perteneciente a el año indicado.
        print('listar bien')
        descargar(lista,anio)#descargamos los reportes de los correspondientes años
        tabla_anio=cargar_performance(tabla_anio,lista,anio)#cargamo las performance de las estaciones
        print('listar bien')
        tabla_anio=cargar_problemas(tabla_anio,lista,anio)#busco que problema tuvo AGGO en las sesiones
        print('listar bien')
        tabla=pd.concat([tabla,tabla_anio])
        print('listar bien')
        tabla.to_csv('salida_funcion.txt',sep=' ')
        print(tabla)
        tabla=cargar_mjd(tabla)
    return tabla

def cargar_mjd(tabla):#cargo el MJD de cada sesión
    tabla['MJD']=tabla.apply(lambda row: convertAmjd(row.anio,row.mes,row.dia),axis=1)
    return tabla

def convertAmjd(anio,mes,dia):#calculo el MJD a partir del año, mes y día
    try:
        mjd=44
        for i in range(1859,int(anio)):
            if bisiesto(i):
                mjd=mjd+366
            else:
                mjd=mjd+365
        for i in range(1,int(mes)):
            if not bisiesto(int(anio)):
                dic={'1':31,'2':28,'3':31,'4':30,'5':31,'6':30,'7':31,'8':31,'9':30,'10':31,'11':30,'12':31}
                mjd=mjd+dic[str(i)]
            else:
                dic={'1':31,'2':29,'3':31,'4':30,'5':31,'6':30,'7':31,'8':31,'9':30,'10':31,'11':30,'12':31}
                mjd=mjd+dic[str(i)]
        mjd=mjd+int(dia)
    except TypeError:
        mjd=np.NaN
    return mjd

def bisiesto(year):
	if year % 4 == 0 and year % 100 != 0 or year % 400 == 0:
		return True
	else:
		return False
		
#Main
anios=['2022'] # Debemos indicar para que años queremos que nos incorpore la información de las sesiones.
tabla=tabla_reports(anios) #creamos tabla
print(tabla)
