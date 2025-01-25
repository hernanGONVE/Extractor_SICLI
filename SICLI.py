
#Librerias requeridas

import zipfile
import pandas as pd
import numpy as np
import time
#================================================================================================
#--------------------------------Valores de entrada----------------------------------------------
#================================================================================================

# Valores de entrada
tabla_1_archivo =["Tabla 1_SICLI_2024"]
tabla_2_archivo =["Tabla 2_SICLI_2024"]
tabla_4_archivos    =   ["202401TABLA04","202402TABLA04","202403TABLA04","202404TABLA04","202405TABLA04","202406TABLA04","202407TABLA04","202408TABLA04","202409TABLA04","202410TABLA04","202411TABLA04"]#Archivos zip con formato txt requerido, no se debe considerar colocar el .zip(VALOR STRING)
tabla_5_archivo =   ["Tabla 5_SICLI_2024"]#Archivos zip con formato txt requerido, no se debe considerar colocar el .zip(VALOR STRING)
ad_seleccionadas = [2,8]  #Areas de demanda solicitadas por la empresa(VALORES ENTEROS)
anio=2024 # Año de la data(VALOR ENTERO)
bisiesto= 1   # 0 para no y 1 para si(VALOR BINARIO, en funcion de la anio ingresado)

#================================================================================================
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#================================================================================================


# Importador de archivos txt desde archivosw comprimidos
def importador_txt(archivo:list):
    dict_meses={}

    for i in range(len(archivo)):
        with zipfile.ZipFile(archivo[i] +   ".zip", 'r') as z:
             with z.open(archivo[i]    +   ".txt") as file:
                m=pd.read_csv(file, sep='\t')
                dict_meses[str(i+1)]  =   m
    return dict_meses

# Funcion modificador de estructura
def formato(df_dict:dict):
    out_dict={}
    for x in list(df_dict.keys()):
        df_dict[x]=(df_dict[x]).rename({"ENERG ACTV":x},axis=1)
        out_dict[x]=df_dict[x][["FECHA",x]]
    return out_dict

#funcion de agrupamiento de elementos
def funcion_agrupar(dic,base):
    for x in list(dic.keys()):
        base=pd.merge(dic[x],base,on="FECHA", how="right")
    
    return base

#Permite detectar si los clientes registran pulsos en mas de una empresa
def detector_empresas(dic):
    una_empresa={}
    for x in list(dic.keys()):
        if len(list(dic[x]["COD EMPRESA"].unique()))    ==   1:
            una_empresa[x]=dic[x]
            dic.pop(x)
    return [una_empresa,dic]

start_time = time.time()

#================================================================================================
time1 = time.time()


tabla_5 =   importador_txt(tabla_5_archivo)
tabla_5 =tabla_5["1"]

dic_list_clientes_ad_selec  = {}
dic_list_clientes_ad_selec_agrupados=[]
for x in ad_seleccionadas :
    tabla_5_ad_selec    =   tabla_5[tabla_5["ID_AREA_DEMANDA"].isin([x])]
    dic_list_clientes_ad_selec[str(x)]   =   list(tabla_5_ad_selec["COD_SUMINISTRO_USUARIO"].unique())
    dic_list_clientes_ad_selec_agrupados+=list(tabla_5_ad_selec["COD_SUMINISTRO_USUARIO"].unique())

print(f"Tiempo de procesamiento de la Tabla 5: {time.time() - time1:.2f} segundos")


tabla_4_df = importador_txt(tabla_4_archivos)

time2 = time.time()

codCL_df_ad_selec ={}
for x in dic_list_clientes_ad_selec_agrupados:
        for y in tabla_4_df.keys():
            if x in codCL_df_ad_selec.keys():
                 codCL_df_ad_selec[x]   =   pd.concat([codCL_df_ad_selec[x],tabla_4_df[y][tabla_4_df[y]["COD SUMINISTRO USUARIO"].isin([x])]],ignore_index=True) # Se concatenan los resultados
            else:
                    codCL_df_ad_selec[x]   =   tabla_4_df[y][tabla_4_df[y]["COD SUMINISTRO USUARIO"].isin([x])]

print(f"Tiempo para la operación de filtrado y reagrupación por código CLXXXXX: {time.time() - time2:.2f} segundos")


time3 = time.time()
# Estructura de ciclos
meses={"01":31,"02":28+bisiesto,"03":31,"04":30,"05":31,"06":30,"07":31,"08":31,"09":30,"10":31,"11":30,"12":31}
horas=["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
minutos=["00","15","30","45"]
Fecha=[]
for mes in list(meses.keys()):
    for dia in range(meses[mes]):
        for hora in horas:
            for minuto in minutos:
                if dia <9:
                    fecha_basica=str(anio)+mes+"0"+str(dia+1)+hora+minuto
                else:
                    fecha_basica=str(anio)+mes+str(dia+1)+hora+minuto
                Fecha+=[fecha_basica]
Fecha=Fecha+[str(anio+1)+"01010000"]

df_base=pd.DataFrame( {"FECHA":Fecha})
df_base=df_base.astype("int64")

print(f"Tiempo para operación de estructuración de fechas: {time.time() - time3:.2f} segundos")


#Agrupamiento de alineados y no alineados
time4 = time.time()
df_alineado={}
df_no_alineado={}
for x in list(codCL_df_ad_selec.keys()):

    if len(list(codCL_df_ad_selec[x]["FECHA"])) == len(list(codCL_df_ad_selec[x]["FECHA"].unique())):
        df_alineado[x]=codCL_df_ad_selec[x]
    else:
        df_no_alineado[x]=codCL_df_ad_selec[x]

df_alineado=formato(df_alineado)
primer_grupo= funcion_agrupar(df_alineado,df_base)

[una_empresa    ,   mas_empresas]=detector_empresas(df_no_alineado)

for n in list(una_empresa.keys()):
    una_empresa[n]=una_empresa[n].drop_duplicates("FECHA",keep="first")
una_empresa


una_empresa=formato(una_empresa)
segundo_grupo=funcion_agrupar(una_empresa,df_base)

consolido_grupos=pd.merge(segundo_grupo,primer_grupo,on="FECHA")
consolido_grupos.set_index("FECHA", inplace=True)


print(f"Tiempo para la operación de información alineada (CL que cumplen con la estructura) y no alineada (CL que no cumplen con la estructura debido a que presentan más de un registro por fecha): {time.time() - time4:.2f} segundos")


time5 = time.time()

for x in dic_list_clientes_ad_selec.keys():
    with pd.ExcelWriter("AD_"+str(x)+"_alineados"+ ".xlsx") as writer:
        requerimiento_alineados = list(set(dic_list_clientes_ad_selec[x]).intersection(consolido_grupos.columns))
        consolido_grupos[requerimiento_alineados].to_excel(writer,sheet_name="AD"+str(x))

print(f"Tiempo para la operación de la exportación de archivos alineados: {time.time() - time5:.2f} segundos")


time6 = time.time()

for x in dic_list_clientes_ad_selec.keys():
    with pd.ExcelWriter("AD_"+str(x)+"_no_alineados"+ ".xlsx") as writer:
        requerimiento_no_alineados = list(set(dic_list_clientes_ad_selec[x]).intersection(mas_empresas.keys()))
        for y in requerimiento_no_alineados:
            mas_empresas[y].to_excel(writer,sheet_name=y)

print(f"Tiempo para la operación de la exportación de archivos no alineados: {time.time() - time6:.2f} segundos")


time7 = time.time()

tabla_1 =importador_txt(tabla_1_archivo)
tabla_2 =importador_txt(tabla_2_archivo)

tabla_1 = tabla_1["1"]
tabla_2 = tabla_2["1"]

tabla_5["Activo"]="si"

pivot_tabla_5 = tabla_5.pivot(index=["COD_EMPRESA","COD_SUMINISTRO_USUARIO","COD_PUNTO_SUMINISTRO","ID_AREA_DEMANDA"], columns="MES_FACTURADO",values="Activo")
pivot_tabla_5 = pivot_tabla_5.fillna('-').reset_index()

tabla_2_apoyo =tabla_2[["COD_SUMINISTRO_USUARIO","NOMBRE_USUARIO_LIBRE","COD_USUARIO_LIBRE"]]
tabla_2_apoyo.drop_duplicates(subset=["COD_USUARIO_LIBRE"], keep="last", inplace=True)
tabla_1_apoyo =tabla_1[["COD_USUARIO_LIBRE","RAZON_SOCIAL"]]
tabla_1_apoyo.drop_duplicates(subset=["COD_USUARIO_LIBRE"], keep="last",inplace=True)
asistencia = pd.merge(tabla_2_apoyo,pivot_tabla_5, right_on="COD_SUMINISTRO_USUARIO",left_on="COD_SUMINISTRO_USUARIO",how="right")
asistencia = pd.merge(tabla_1_apoyo,asistencia,right_on="COD_USUARIO_LIBRE",left_on="COD_USUARIO_LIBRE",how="right")

pd.options.display.float_format = '{:.0f}'.format

asistencia.drop_duplicates(inplace=True)

asistencia["ID_AREA_DEMANDA"].unique()

with pd.ExcelWriter("Asistencia_"+str(anio)+ ".xlsx") as writer:

    for x in asistencia["ID_AREA_DEMANDA"].unique():
        asistencia[asistencia["ID_AREA_DEMANDA"]==x].to_excel(writer,sheet_name="AD-"+str(x))

print(f"Tiempo para la operación para el cálculo de asistente AD, información de apoyo: {time.time() - time7:.2f} segundos")
print("================================================================")
print("****************************************************************")
print("---------------------Proceso finalizado-------------------------")
print("****************************************************************")
print("================================================================")
