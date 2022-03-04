# _author_ = "Diego Alves"
# _author_ = "Adilton Costa"
# _author_ = "Sandi Ourique"
# _version_ = "0.0.1"

from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import *  
from pyspark.sql.types import FloatType
from time import time

    

if __name__=="__main__":


    """
        Script que lê uma base de dados .csv contendo informações de remunerações de aposentados do BACEN do mês 09/2021
        realiza a filtragem de dados relevantes, tratamento e análise de informações obtidas retornando um parquet.
             
    """

    spark = (SparkSession
    .builder
    .appName("analise_aposentados_sandi")
    .master("spark://16.171.35.44:7077")
    .config("spark.driver.memory","4g")
    .config("spark.driver.cores",'4')
    .config("spark.executor.memory", '4g')
    .config("spark.executor.cores", '4')
    .config("spark.cores.max", '4')
    .getOrCreate())
    
    
    # leitura do arquivo CSV
    dados = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load("202109_Remuneracao.csv")
    dados_cadastro = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load("202109_Cadastro.csv")
   

    tempo_inicial = time()

    dados.show()

    print("tempo de execução", time()-tempo_inicial)


    # Nesse trecho fazemos a filtragem dos dados relevantes e atribuimos a um novo DataFrame para análise a posteriore
    dados_novo = dados.select(  dados["ANO"].alias("ano"),
                                dados["MES"].alias("mes"),
                                dados["Id_SERVIDOR_PORTAL"].alias("id"),
                                dados["NOME"].alias("nome"),
                                dados["CPF"].alias("cpf"),
                                dados["REMUNERAÇÃO BÁSICA BRUTA (R$)"].alias("remu_bruta"),
                                dados["GRATIFICAÇÃO NATALINA (R$)"].alias("grat_natal"),
                                dados["FÉRIAS (R$)"].alias("ferias")
                        
    )

    dados_novo_cadastro = dados_cadastro.select(dados_cadastro["Id_SERVIDOR_PORTAL"].alias("id"),
                                                dados_cadastro["NOME"].alias("nome"),
                                                dados_cadastro["DESCRICAO_CARGO"].alias("cargo"),
                                                dados_cadastro["ORG_LOTACAO"].alias("organizacao"),
                                                dados_cadastro["DATA_INGRESSO_CARGOFUNCAO"].alias("ingresso"),
                                                dados_cadastro["JORNADA_DE_TRABALHO"].alias("jornada")
                        
    )

  
    
    # O novo DataFrame é salvo em formato parquet
    
    dados_novo.write.mode("overwrite").parquet("remuneracao.parquet")
    dados_novo_cadastro.write.mode("overwrite").parquet("cadastro.parquet")

    dados_p = spark.read.parquet("remuneracao.parquet")
    dados_cadastro_p = spark.read.parquet("cadastro.parquet")

    # Fazemos uso do regexp_replace para normalizar os dados
    dados_p = dados_p.withColumn("remu_bruta", regexp_replace("remu_bruta", "," , "."))
    dados_p = dados_p.withColumn("grat_natal", regexp_replace("grat_natal", "," , "."))
    dados_p = dados_p.withColumn("ferias", regexp_replace("ferias", "," , "."))

    # É feita a conversão de dados para o tipo 'float' para processamento
    dados_p = dados_p.withColumn("remu_bruta", col("remu_bruta").cast(FloatType()))
    dados_p = dados_p.withColumn("grat_natal", col("grat_natal").cast(FloatType()))
    dados_p = dados_p.withColumn("ferias", col("ferias").cast(FloatType()))
    
    print("Total de Registros", dados_novo.count())
    print("Anos faltantes", dados_novo.filter(dados_novo["ano"].isNull()).count())
    print("Mês faltantes", dados_novo.filter(dados_novo["mes"].isNull()).count())
    print("Id faltantes", dados_novo.filter(dados_novo["id"].isNull()).count())
    print("Nome faltantes",dados_novo.filter(dados_novo["nome"].isNull()).count())
    print("CPF faltantes", dados_novo.filter(dados_novo["cpf"].isNull()).count())
    print("Remuneração Bruta faltantes", dados_novo.filter(dados_novo["remu_bruta"].isNull()).count())
    print("Gratificação de Natal", dados_novo.filter(dados_novo["grat_natal"].isNull()).count())
    print("Férias", dados_novo.filter(dados_novo["ferias"].isNull()).count())
    print("Valor Nulo", dados_novo.filter(dados_novo["ferias"].isNull()).show())

    # Removemos valores nulos
    dados_p = dados_p.dropna()
    
    # Ordena a coluna Remuneração bruta, com ordem decrescente conforme os valores de remuneração
    print("Apresentação dos 3 servidores com maior Remuneração Bruta, conforme valores em ordem decrescente")
    dados_p.orderBy(col("remu_bruta").desc()).show(3, truncate=False)

  
    # Filtra o servidor pelo ID na tabela de casatro -  cujo a remuneração é meior na tabela de remuneração. 
    print("Apresentação dos dados referente ao ID do servidor obteve maior remuneração bruta")
    dados = dados_novo_cadastro.filter(col("id") == "79205664")    

    print(dados.show(truncate=False))
    
    
    
    input()

