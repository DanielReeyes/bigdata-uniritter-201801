# Dados de voos já utilizados em outra disciplina.
# Esta versão possui algumas diferenças em nomes de colunas
install.packages( "nycflights13" )

format( object.size(nycflights13::flights), "MB" )

library(sparklyr)
install.packages("tidyverse")
library(tidyverse)

# Inicializa processo do Spark
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark.
# Faz uma cópia do dataset para dentro do Spark guardando na estrutura flights
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")

# Lista tabelas do catálogo
src_tbls(sc)

# Abre a página principal da interface gráfica do Spark. 
# É possivel verificar na pagina do navegador os dados
spark_web(sc)

# Summary funciona?
# Trará as informações do objeto list, não do dataframe do Spark
summary(flights_tbl)

# Neste caso precisamos de uma função diferente.
# Aqui ele fará o summary com os dados do dataframe no Spark
# sdf = Spark Data Frame
# Aplica uma série de jobs para cada uma das colunas
# Cada job indica desvio padrão, média, min, max e etc..
# Podendo ser visto na página do navegador também
sdf_describe(flights_tbl)

spark_disconnect(sc)
