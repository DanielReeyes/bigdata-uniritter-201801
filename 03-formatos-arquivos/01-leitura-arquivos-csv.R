library(sparklyr)
options(sparklyr.java9 = TRUE)
sc <- spark_connect(master = "local")
library(tidyverse)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE )

head(ted_main)
#Exibirá inúmeros campos null
#Mas se abrirmos o arquivo dentro da pasta "data" terá várias linhas com "," 
#indicando que supostamente estariam separando colunas

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE, delimiter = ",", quote = '"', escape = '\\')
#Também não permitirá separar por outros caracteres especiais

head(ted_main)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE
                          , columns = c( "comments", "description", "duration", "event", "film_date", "languages", "main_speaker", "name"
                                       , "num_speaker", "published_date", "speaker_occupation", "title", "url", "views" ))

#Não permitirá também ler por colunas, pois o spark necessitará processar o arquivo primeiro pois primeiro necessita fazer o quote, 
#indicando até onde vai cada coluna

local_ted_main <- read_csv("03-formatos-arquivos/data/ted_main.csv")
#Utilizando a biblioteca do R irá funcionar pois a implementação do R 
#permite importar o arquivo para depois limpar

head(local_ted_main)

local_ted_main %>%
  mutate(url = str_replace_all(url, "\n", "")) %>%
  mutate_if(is_character, ~ str_replace_all(.x, ",", " ")) %>%
  select(-ratings, -related_talks, -tags) %>%
  write_csv("03-formatos-arquivos/data/ted_main_no_json.csv")
#Limpa o CSV com mais de um quote

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main_no_json.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE )
#Depois de limpar o CSV sim ele permitirá utilizar o spark para ler o arquivo CSV

head(ted_main)

ted_main %>%
  mutate_at(vars(comments, duration, languages, published_date), as.integer) %>% 
  head()

spark_disconnect(sc)
