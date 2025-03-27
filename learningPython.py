import pandas as pd
import numpy as np

dados = pd.read_csv("data/dados.csv")
dados

# OBJETIVO: separar a coluna score

#filtrando a base para o ano de 2023
temporada = 2023
dados_temporada = dados.query("season == @temporada")

# criando uma função que separa strings
def pegar_gols_mandante(placar):
    return placar.split(sep = "x")[0]


def pegar_gols_visitante(placar):
    return placar.split(sep = "x")[1]


def verificar_vitoria_mandante(placar):
    if pegar_gols_mandante(placar) < pegar_gols_visitante(placar):
        return 1
    elif pegar_gols_mandante(placar) > pegar_gols_visitante(placar):
        return 2
    else: 
        return 0


def verificar_vitoria_visitante(placar):
    if pegar_gols_mandante(placar) < pegar_gols_visitante(placar):
        return 2
    elif pegar_gols_mandante(placar) > pegar_gols_visitante(placar):
        return 1
    else: 
        return 0

def calcular_pontos_mandante(placar):
    if verificar_vitoria_mandante(placar) == 2:
        return 3
    elif verificar_vitoria_visitante(placar) == 2:
        return 0
    else: 
        return 1     


def calcular_pontos_visitante(placar):
    if verificar_vitoria_mandante(placar) == 2:
        return 0
    elif verificar_vitoria_visitante(placar) == 2:
        return 3
    else: 
        return 1     



# agora aplicaremos as função criando colunas de gols
# para mandantes e visitantes com o método assing()
dados_temporada = (
    dados_temporada
    .assign(
        gols_mandante = lambda x: x["score"].apply(pegar_gols_mandante),
        gols_visitante = lambda x: x["score"].apply(pegar_gols_visitante),
        vitoria_mandante = lambda x: x["score"].apply(verificar_vitoria_mandante),
        vitoria_visitante = lambda x: x["score"].apply(verificar_vitoria_visitante),
        pontos_mandante = lambda x: x["score"].apply(calcular_pontos_mandante),
        pontos_visitante = lambda x: x["score"].apply(calcular_pontos_visitante)
    )
)
# alterando o tipo dos dados
dados_temporada[["gols_mandante", "gols_visitante"]] = dados_temporada[["gols_mandante", "gols_visitante"]].astype("int")

# Criando uma tabela com dados de times mandantes
tab_mandante = (
    dados_temporada
    .filter(items=["home", 
    "pontos_mandante", 
    "vitoria_mandante", 
    "gols_mandante",
    "gols_visitante"]
    )
    .rename(columns={
        "home": "time", 
        "pontos_mandante": "pontos", 
        "vitoria_mandante": "vitorias", 
        "gols_mandante": "gols_marcados",
        "gols_visitante": "gols_sofridos"}
    )
)

# Criando uma tabela com dados de times visitantes
tab_visitante = (
    dados_temporada
    .filter(items=["away",
    "pontos_visitante", 
    "vitoria_visitante", 
    "gols_visitante",
    "gols_mandante"]
    )
    .rename(columns={"away": "time", 
    "pontos_visitante": "pontos", 
    "vitoria_visitante": "vitorias", 
    "gols_visitante": "gols_marcados",
    "gols_mandante": "gols_sofridos"}
    )
)

#criando as tabelas de gols sofridos por mando
tab_gols_sofridos_mandante = (
    tab_mandante
    .groupby("time")["gols_sofridos"]
    .agg("sum")
    .sort_values(ascending=False)
    .reset_index()
)


tab_gols_sofridos_visitante = (
    tab_visitante
    .groupby("time")["gols_sofridos"]
    .agg("sum")
    .sort_values(ascending=False)
    .reset_index()
)

# concatenando as tabelas
tab_concat = pd.concat([tab_mandante, tab_visitante])

#Criando a tabela de gols sofridos total
tab_gols_sofridos = (
    tab_concat
    .groupby("time")["gols_sofridos"]
    .agg("sum")
    .sort_values(ascending=False)
    .reset_index()
)


# Agora posso agrupar por time e analisar os dados
tab_pontos = (
    tab_concat
    .groupby("time")["pontos"]
    .agg("sum")
    .sort_values(ascending=False)
    .reset_index()
)

tab_vitorias = (
    tab_concat
    .query("vitorias == 2")
    .groupby("time")["vitorias"]
    .agg("count")
    .sort_values(ascending=False)
    .reset_index()
)


tab_derrotas = (
    tab_concat
    .query("vitorias == 1")
    .groupby("time")["vitorias"]
    .agg("count")
    .sort_values(ascending=False)
    .reset_index()
)

tab_empates = (
    tab_concat
    .query("vitorias == 0")
    .groupby("time")["vitorias"]
    .agg("count")
    .sort_values(ascending=False)
    .reset_index()
)


tab_gols_marcados = (
    tab_concat
    .groupby("time")["gols_marcados"]
    .agg("sum")
    .sort_values(ascending=False)
    .reset_index()
)

#Saldo de gols
tab_saldo_de_gols = (
    tab_gols_marcados
    .merge(tab_gols_sofridos,
    on = "time",
    )
    .assign(
        saldo_de_gols = lambda x: x["gols_marcados"] - x["gols_sofridos"])
)

tab_jogos = (
    tab_concat
    .groupby("time")["pontos"]
    .agg("count")
    .reset_index()
)


classificacao = (
    tab_pontos
    .merge(tab_jogos, on = "time")
    .merge(tab_vitorias, on = "time")
    .merge(tab_empates, on = "time")
    .merge(tab_derrotas, on = "time")
    .merge(tab_saldo_de_gols, on = "time")
)

classificacao.columns = [
    "clube", "pts", "pj", 
    "vit", "emp","der",
    "gm","gc","sg"
]

print(classificacao)