# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 21:22:05 2025

@author: thiagop
"""

import os
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)


def MatchVehicleType(vehicle_type):
    valid_types = {
        ' Undefined':"Indefinido",
        ' Car':"Carro",
        ' Medium Vehicle':"Carro",
        ' Heavy Vehicle':"Carro",
        ' Motorcycle':"Moto",
        ' Light Truck':"Caminhão",
        ' Bus':"Ônibus",
        ' Van':"Carro",
        ' Truck':"Caminhão",
        ' Cartrailer':"Carro",
        ' Tractor':"Caminhão",
        ' Trucktrailer':"Carro",
        ' Pedestrian':"Pedestre",
        ' Bicycle':"Bicicleta",
        ' Animal':"Animal",
        ' Caravan':"Carro",
        }
    
    if not vehicle_type in list(valid_types.keys()):
        return None
    
    return valid_types[vehicle_type]   

def ConcatSequentialRecords(file_list):
    
    df_list = []
    last_instant = 0
    
    for i in file_list:
        df_ = pd.read_csv(i,sep=';',encoding='utf-8',encoding_errors="ignore")
        df_.insert(0,"Arquivo",os.path.basename(i))
        df_[" Track ID"] = df_["Arquivo"].astype(str) + "-" + df_[" Track ID"].astype(str)
        df_[" Entry Time [s]"] = df_[" Entry Time [s]"] + last_instant
        last_instant = df_[" Entry Time [s]"].max()
        
        df_list.append(df_)
    
    df = pd.concat(df_list,ignore_index=True).sort_values(" Entry Time [s]")
    
    return df

def AggOD(file_list,n_min=15,f_corr=None,f_corr_perc=1,):
    # Le, contatena e compatibiliza os arquivos
    df = ConcatSequentialRecords(file_list)
    # Compatibilização dos tipos de veículos
    df["Tipo de Veículo"] = df[" Track Type"].apply(MatchVehicleType)
    # Remover pedestres e ciclistas
    df = df[-df["Tipo de Veículo"].isin(["Pedestre","Bicicleta"])]
    # Ordenar por instante de entrada
    df = df.sort_values(by=" Entry Time [s]")
    
    # Região de entrada (primeira região)
    # Região de saída (última região)
    df_agg = df.groupby([" Track ID"]).agg({
        " Entry Time [s]":"min",
        "Arquivo":"first",
        "Traffic Region ID":["first","last"],
        "Tipo de Veículo":"first"
        }).reset_index(drop=False)
    
    # Ajuste da hierarquia de colunas
    df_agg.columns = df_agg.columns.droplevel(1)+" "+df_agg.columns.droplevel(0)

    # Renomear colunas
    df_agg = df_agg.rename(columns={
        " Track ID ":"ID",
        "Arquivo first":"Arquivo",
        " Entry Time [s] min":"Instante (s)",
        "Traffic Region ID first":"Origem",
        "Traffic Region ID last":"Destino",
        "Tipo de Veículo first":"Tipo de Veículo"
        })
    
    # Calculo agrupado por n_min
    df_agg["Grupo Horário"] = df_agg["Instante (s)"] / (n_min*60)
    df_agg["Grupo Horário"] = df_agg["Grupo Horário"].astype(int).astype(str)
    
    # Verificação e cálculo do fator de correção
    if f_corr==None:
        # Total de veículos detectados
        count_all_detections = len(df_agg)
        
        # Remover pares sem sentido (origem = destino)
        df_agg = df_agg[df_agg["Origem"]!=df_agg["Destino"]]
        
        # Calcula o fator de correção
        f_corr = count_all_detections/len(df_agg)
    else:
        # Remover pares sem sentido (origem = destino), fator de correção explícito
        df_agg = df_agg[df_agg["Origem"]!=df_agg["Destino"]]
    
    print(f"Fator de Correção = {f_corr}")
    
    # Par origem-destino-horario
    df_agg["Par ODH"] = df_agg["Origem"].astype(str) + "-" + df_agg["Destino"].astype(str) + "-" + df_agg["Grupo Horário"].astype(str)
    df_agg = df_agg.sort_values("Instante (s)")
    
    # Salvar
    df_agg.to_excel(os.path.join(os.path.dirname(file_list[0]),f"Dados_Concatenada_{n_min}min.xlsx"))
    
    # Classes contabilizadas
    vehicle_type_list = ["Moto","Carro","Caminhão","Ônibus"]
    
    df_count = pd.DataFrame()
    for i in vehicle_type_list:
        df_count[i] = df_agg.groupby("Par ODH").apply(lambda x: x[x["Tipo de Veículo"]==i]["Tipo de Veículo"].count())
    for i in df_agg["Arquivo"].unique():
        df_count[i] = df_agg.groupby("Par ODH").apply(lambda x: x[x["Arquivo"]==i]["Arquivo"].count())
    
    # Ajuste dos fatores de correção e arredondamento
    df_count = df_count*f_corr*f_corr_perc
    df_count = df_count.round(0).astype(int)
    
    # Salvar
    df_count.to_excel(os.path.join(os.path.dirname(file_list[0]),f"CVC_OD_Concatenada_{n_min}min.xlsx"))

def CountByRegion(file_list,f_corr_perc=1):
    # Le, contatena e compatibiliza os arquivos
    df = ConcatSequentialRecords(file_list)
    # Compatibilização dos tipos de veículos
    df["Tipo de Veículo"] = df[" Track Type"].apply(MatchVehicleType)
    # Remover pedestres e ciclistas
    df = df[-df["Tipo de Veículo"].isin(["Pedestre","Bicicleta"])]
    # Ordenar por instante de entrada
    df = df.sort_values(by=" Entry Time [s]")

    # Classes contabilizadas
    vehicle_type_list = ["Moto","Carro","Caminhão","Ônibus"]
    
    df_count = pd.DataFrame()
    for i in vehicle_type_list:
        df_count[i] = df.groupby("Traffic Region ID").apply(lambda x: x[x["Tipo de Veículo"]==i]["Tipo de Veículo"].count())
    for i in df["Arquivo"].unique():
        df_count[i] = df.groupby("Traffic Region ID").apply(lambda x: x[x["Arquivo"]==i]["Arquivo"].count())

    # Ajuste dos fatores de correção e arredondamento
    df_count = df_count*f_corr_perc
    df_count = df_count.round(0).astype(int)
    
    # Salvar
    df_count.to_excel(os.path.join(os.path.dirname(file_list[0]),f"CVC_Agg_por_Regiao_Concatenada.xlsx"))

if __name__=="__main__":
    file_list = [
        r"C:\Users\User\Desktop\Contagem CE-060\Drone Tarde\DJI_0654_comprimido_ffmpeg_mp4.csv",
        r"C:\Users\User\Desktop\Contagem CE-060\Drone Tarde\DJI_0657_comprimido_ffmpeg_mp4.csv",
        r"C:\Users\User\Desktop\Contagem CE-060\Drone Tarde\DJI_0658_comprimido_ffmpeg_mp4.csv",
        r"C:\Users\User\Desktop\Contagem CE-060\Drone Tarde\DJI_0660_comprimido_ffmpeg_mp4.csv",
        ]
    
    print(f"Processando arquivos... {len(file_list)}.")
    
    CountByRegion(file_list)
    AggOD(file_list,n_min=60)

    print(f"Arquivos processados {len(file_list)}.")