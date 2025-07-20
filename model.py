import os
import pandas as pd
import warnings
# import xlsxwriter
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)

def AggOD_old(file_list,n_min=15,f_corr=None,f_corr_perc=1,vehicle_type_list=["Moto","Carro","Caminhão","Ônibus"]):
    """
    Agrega os dados em uma matrix OD de n_min
    """
    # Le, contatena e compatibiliza os arquivos
    df = ConcatSequentialRecords(file_list)
    # Compatibilização dos tipos de veículos
    df["Tipo de Veículo"] = df[" Track Type"].apply(MatchVehicleType)
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
    
    # Ajuste do nome das colunas pela hierarquia de colunas
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
    
    print(f"Fator de Correção = {round(f_corr,2)}")
    
    # Par origem-destino-horario
    df_agg["Par ODH"] = df_agg["Origem"].astype(str) + "-" + df_agg["Destino"].astype(str) + "-" + df_agg["Grupo Horário"].astype(str)
    df_agg = df_agg.sort_values("Instante (s)")
    
    # Salvar
    df_agg.to_excel(os.path.join(os.path.dirname(file_list[0]),f"Dados_Concatenada_{n_min}min.xlsx"))
    
    df_count = df_agg.groupby("Par ODH").agg({"Instante (s)":"count"}).rename(columns={"Instante (s)":"Total"})
    df_count["UCP"] = 0
    for i in vehicle_type_list:
        df_count[i] = df_agg.groupby("Par ODH").apply(lambda x: x[x["Tipo de Veículo"]==i]["Tipo de Veículo"].count())
        df_count["UCP"] = df_count["UCP"] + df_count[i]*ValueUCP(i)
    
    for i in df_agg["Arquivo"].unique():
        df_count[i] = df_agg.groupby("Par ODH").apply(lambda x: x[x["Arquivo"]==i]["Arquivo"].count())

    # Ajuste dos fatores de correção e arredondamento
    df_count = df_count*f_corr*f_corr_perc
    df_count = df_count.round(0).astype(int)
    df_count = df_count.reset_index(drop=False)

    # Separa ODH
    df_count.insert(1,"H",df_count["Par ODH"].apply(lambda x:x.split("-")[2]))
    df_count.insert(1,"D",df_count["Par ODH"].apply(lambda x:x.split("-")[1]))
    df_count.insert(1,"O",df_count["Par ODH"].apply(lambda x:x.split("-")[0]))
    
    # Salvar
    df_count.to_excel(os.path.join(os.path.dirname(file_list[0]),f"CVC_OD_Concatenada_{n_min}min.xlsx"),index=False)

    return df_count

def CountByRegion_old(file_list,f_corr_perc=1,vehicle_type_list=["Moto","Carro","Caminhão","Ônibus"]):
    # Le, contatena e compatibiliza os arquivos
    df = ConcatSequentialRecords(file_list)
    # Compatibilização dos tipos de veículos
    df["Tipo de Veículo"] = df[" Track Type"].apply(MatchVehicleType)
    # Ordenar por instante de entrada
    df = df.sort_values(by=" Entry Time [s]")
    
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

def MatchVehicleType(vehicle_type):
    """
    Padronizar rótulos
    """
    valid_types = {
        ' Undefined':"Indefinido",
        ' Car':"Carro",
        ' Medium Vehicle':"Carro",
        ' Heavy Vehicle':"Caminhão",
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
    """
    Concatena dataframes e reseta o horário de cada sequencialmente
    A sequência na ordem da lista
    """
    df_list = []
    # first value
    last_instant = 0
    
    for i in file_list:
        df_ = pd.read_csv(i,sep=';',encoding='utf-8',encoding_errors="ignore")
        df_.columns = [i.strip() for i in df_.columns]
        df_.insert(0,"File",os.path.basename(i))
        df_["Track ID"] = df_["File"].astype(str) + "_" + df_["Track ID"].astype(str)
        df_["Entry Time [s]"] = df_["Entry Time [s]"] + last_instant
        df_list.append(df_)

        # update value
        last_instant = df_["Entry Time [s]"].max()
        
    
    df = pd.concat(df_list,ignore_index=True).sort_values("Entry Time [s]")
    
    
    return df

def ValueUCP(vehicle_class):
    UCP_class = {
        "Carro":1,
        "Moto":1/3,
        "Caminhão":2,
        "Ônibus":2,
        }

    if not vehicle_class in UCP_class.keys():
        return 0
    return UCP_class[vehicle_class]

def AggODByRegion(folder_path,drop_vehicle_types=[],file_extension="csv",override=False):
    os.chdir(folder_path)

    if os.path.isfile("DFS_processed.xlsx") or override:
        if not override:
            if str(input("Já existe um arquivo processado. Prosseguir? (y/n)\t")).lower()!="y":
                print("Execução cancelada!")
                return None

    file_list = os.listdir(folder_path)
    file_list = [f for f  in file_list if f.endswith(f".{file_extension}")]

    if len(file_list)==0:
        print(f"A pasta '{folder_path}' não apresenta arquivos no formato '.{file_extension}'.")
        return None

    df = ConcatSequentialRecords(file_list)
    df["Track Type"] = df["Track Type"].apply(MatchVehicleType)
    df = df[-df["Track Type"].isin(drop_vehicle_types)]

    df = df.sort_values(by="Entry Time [s]")

    df = df.groupby(["Track ID"]).agg({
        "Entry Time [s]":["first","last"],
        "File":"first",
        "Traffic Region ID":["first","last"],
        "Track Type":"first"
        }).reset_index(drop=False)
    df.columns = df.columns.droplevel(1) + " " + df.columns.droplevel(0)

    df["Weight"] = df["Track Type first"].apply(ValueUCP)

    # df_count = pd.DataFrame()
    # for i in df["Track Type"].unique():
    #     df_count[i] = df.groupby("Traffic Region ID").apply(lambda x: x[x["Tipo de Veículo"]==i]["Tipo de Veículo"].count())

    # Save excel
    # Creating Excel Writer Object from Pandas  
    with pd.ExcelWriter(
        "DFS_processed.xlsx",
        engine="xlsxwriter",
        engine_kwargs={"options":{"strings_to_formulas":False,"strings_to_urls": False}}
        ) as writer:

        workbook = writer.book
        worksheet = workbook.add_worksheet('Origin-destination pair')
        writer.sheets['Origin-destination pair'] = worksheet
        df.to_excel(writer,sheet_name='Origin-destination pair',startrow=0,startcol=0,index=False) 

    return df

if __name__=="__main__":
    folder_path = r"D:\Certare Engenharia (Local)\72 - Plano de Segurança Viária\CE-065-1\Simulação\Rua Dr Argeu Braga Herbster (triângulo)"
    result = AggODByRegion(folder_path)
    print(result)