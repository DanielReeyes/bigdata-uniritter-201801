library(sparklyr)
sc <- spark_connect(master = "local")

library(tidyverse)

# 1 ratings cada linha é uma instancia de dado
ratings_music <- spark_read_json(sc, name = "ratings_music", path = "03-formatos-arquivos/data/subset_meta_Digital_Music.json.gz", memory = FALSE)
# 2 ratins o arquivo JSON estará formatado, dessa maneira quando utilizado no SPARK irá se perder pois o arquivo ficará todo quebrado
# ratings_music_ <- spark_read_json(sc, name = "ratings_music_", path = "03-formatos-arquivos/data/pretty_Digital_Music.json.gz", memory = FALSE)

#O Spark irá funcionar com arquivo JSON quando o mesmo estiver com o formato de linhas


sdf_schema(ratings_music)

# sdf_schema(ratings_music_)
# DBI::dbRemoveTable(sc, "ratings_music_")

head(ratings_music)

#Filtra pelo identificador "asin"
ratings_music %>%
  filter(asin == "6308051551") %>%
  collect() %>%
  View()

# Quais são as 100 marcas (brands) mais comuns? Das 100 mais comuns, exiba todas aquelas com no mínimo 2 ocorrências e mostre na console a query resultante desta análise
#print(n = 100) indica para imprimir apenas 100 (caso haja mais do que isso)
ratings_music %>%
  count(brand) %>%
  arrange(desc(n)) %>%
  head(100) %>%
  filter(n > 1) %>%
  collect() %>%
  print(n = 100)

ratings_music %>%
  count(brand) %>%
  arrange(desc(n)) %>%
  head(100) %>%
  filter(n > 1) %>% show_query()

install.packages("devtools")
devtools::install_github("chezou/sparkavro")
#copiar jar databricks avro para as libs da cópia local do spark (ver local no sc)
devtools::install_github("mitre/sparklyr.nested")

library(sparkavro)

#converte o JSON para o arquivo apache (próximo tipo de arquivo)
spark_write_avro(ratings_music, path = "03-formatos-arquivos/data/subset_meta_Digital_Music.avro", options = list(compression = "snappy"))

spark_disconnect(sc)
spark_disconnect_all()
