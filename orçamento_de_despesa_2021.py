# _author_ = "Diego Alves"
# _author_ = "Adilton Costa"
# _author_ = "Sandi Ourique"
# _version_ = "0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


"""
    Script que lê uma base de dados .csv contendo informações de orçamento de despesa 2021
    realiza a filtragem de dados relevantes, tratamento e análise de informações obtidas retornando um parquet.

"""


if __name__=="__main__":

    spark = SparkSession.builder.appName("sandi").config("spark.sql.caseSensitive", "True").getOrCreate()
    
    # spark = (SparkSession
    # .builder
    # .appName("analise-orcamento_despesa_sandi")
    # .master("spark://16.171.35.44:7077")
    # .config("spark.driver.memory","6g")
    # .config("spark.driver.cores",'6')
    # .config("spark.executor.memory", '6g')
    # .config("spark.executor.cores", '6')
    # .config("spark.cores.max", '6')
    # .getOrCreate())
    
    # leitura do arquivo CSV
    dados = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load("2021_OrcamentoDespesa.csv")
    print("\n"*10)
    
    # editando os cabeçalhos
    dados_novo = dados.select(  dados["EXERCÍCIO"].alias("exercicio"),
                                dados["CÓDIGO ÓRGÃO SUPERIOR"].alias("codigo_org_superior"),
                                dados["NOME ÓRGÃO SUPERIOR"].alias("nome_org_superior"),
                                dados["NOME GRUPO DE DESPESA"].alias("nome_grupo_despesa"),
                                dados["ORÇAMENTO INICIAL (R$)"].alias("orcamento_inicial"),
                                dados["ORÇAMENTO ATUALIZADO (R$)"].alias("orcamento_atualizado"),
                                dados["ORÇAMENTO EMPENHADO (R$)"].alias("orcamento_empenhado"),
                                dados["ORÇAMENTO REALIZADO (R$)"].alias("orcamento_realizado")
    
    )
    # dados_novo.printSchema()

    dados_novo.write.mode("overwrite").parquet("data.parquet")
    dados_p = spark.read.parquet("data.parquet")

    # dados_p.printSchema()

    # verificando dados nulos

    print("Total de dados: ", dados_p.count())
    print("Dados nulos em exercicio", dados_p.filter(dados_p["exercicio"].isNull()).count())
    print("Dados nulos em código do órgão superior", dados_p.filter(dados_p["codigo_org_superior"].isNull()).count())
    print("Dados nulos em nome órgão superior", dados_p.filter(dados_p["nome_org_superior"].isNull()).count())
    print("Dados nulos em nome do gurpo de despesa", dados_p.filter(dados_p["nome_grupo_despesa"].isNull()).count())
    print("Dados nulos em orçamento inicial", dados_p.filter(dados_p["orcamento_inicial"].isNull()).count())
    print("Dados nulos em orçamento atualizado", dados_p.filter(dados_p["orcamento_atualizado"].isNull()).count())
    print("Dados nulos em orçamento empenhado", dados_p.filter(dados_p["orcamento_empenhado"].isNull()).count())
    print("Dados nulos em orçamento realizado", dados_p.filter(dados_p["orcamento_realizado"].isNull()).count())

    # editando as colunas com vírgula substituindo por ponto
    dados_p = dados_p.withColumn("orcamento_inicial", regexp_replace("orcamento_inicial", "," , "."))
    dados_p = dados_p.withColumn("orcamento_atualizado", regexp_replace("orcamento_atualizado", "," , "."))
    dados_p = dados_p.withColumn("orcamento_empenhado", regexp_replace("orcamento_empenhado", "," , "."))
    dados_p = dados_p.withColumn("orcamento_realizado", regexp_replace("orcamento_realizado", "," , "."))
    
    # É feita a conversão de dados para o tipo 'Numeric' para processamento
    dados_p = dados_p.withColumn("orcamento_inicial", col("orcamento_inicial").cast(DoubleType()))

    # Agrupando os dados do orgão e aplicando calculos
    print("Apresentação dos orçamento inicial vs orçamento realizado")
    dados_tratados = dados_p.groupBy("nome_org_superior").agg(max("orcamento_inicial").alias("orcamento inicial 2021"), max("orcamento_realizado").alias("orcamento realizado 2021")).show(truncate=False)
    
    print("Apresentação da média de orçamento inicial geral vs orçamento realizado geral")
    ados_tratados = dados_p.agg(avg("orcamento_inicial"), avg("orcamento_realizado")).show(truncate=False)
    
    # Verificando o orçamento realizado de maior valor e qual órgão superior 
    print("Apresentação dos orgão superior com maior valor inicial.")
    dados_p.groupBy("nome_org_superior").agg(sum("orcamento_inicial")).orderBy(max("orcamento_inicial").desc()).show(2, truncate=False)
    

    # Apresentando detalhamento de gastos
    print("Detalhamento dos gastos realizados pelo Ministério da Economia")
    dados_p.filter("nome_org_superior == 'Ministério da Economia'").show(50, truncate=False)

    # dados_p.show(truncate=False)



