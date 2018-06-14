library(sparklyr)
sc <- spark_connect(master = "local")

library(tidyverse)
library(sparkavro)
library(sparklyr.nested)

ratings_music <- spark_read_parquet(sc, name = "ratings_music", path = "03-formatos-arquivos/data/subset_meta_Digital_Music.parquet", memory = FALSE)

ratings_music %>%
  select(asin, description, imUrl, price, title) %>%
  show_query()

ratings_music %>%
  select(asin, description, imUrl, price, title)

#Pega as colunas quando preço maior que 15 Obs.: Olhar no navegador o que foi feito no spark  
#Filtra primeiro os possíveis dados e depois faz os selects necessários
ratings_music %>%
  select(asin, description, imUrl, price, title) %>%
  filter(price > 15)

spark_disconnect_all()
