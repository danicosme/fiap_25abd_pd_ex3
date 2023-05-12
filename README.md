# <b>Análise exploratória no PySpark</b>
<i>MBA Engenharia de Dados FIAP | Turma 25ABD<br>
Repositório para projeto do exercício 3 da matéria de processamento e armazenamento distribuído de dados do MBA de Engenharia de Dados.</i><br><br>

O ambiente de Big Data utilizado neste projeto está disponível no seguinte repositório:
- https://github.com/fabiogjardim/bigdata_docker

## <b>1. Dataset</b>
O dataset utilizado para essa análise exploratória foi o YouTube Trending Video Dataset, disponível em: https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset.

Esse dataset possui as informações de vídeos em alta e categorias de vídeos separados por país, sendo estes: 
  - Alemanha
  - Brasil
  - Canadá
  - Coréira do Sul
  - Estados Unidos
  - França
  - Grã-Bretanha
  - Índia
  - Japão
  - México
  - Rússia

## <b>2. Ingestao arquivos para HDFS </b>
Para facilitar a implementação, desenvolvemos dois scrpits:

<b>1) move-dataset-to-container.sh:</b> copia o dataset e o script move-dataset-to-hdfs.sh para a pasta datasets dentro do container namenode. O comando utilizado neste script foi:
><i>docker cp [origem_arquivo] [id_container]:[destino_arquivo]</i>

<b>2) move-dataset-to-hdfs.sh:</b> faz a criação da pata no HDFS para armazenar os arquivos .csv e .json e os copia para dentro da pasta. Os comandos utilizados neste script são:
><i>hadoop fs -mkdir [nome_diretorio]</i> 
><i>hadoop fs -put [diretorio_origem] [diretorio_destino]</i>

## <b>3. Execução dos scripts</b>

### <b>Screenshots</b>

- Antes da execução dos scripts
![image](https://github.com/danicosme/fiap_25abd_pd_ex3/blob/main/prints/dir-antes-scripts.png)

- Acessar a pasta local onde estão os arquivos e colocar os scripts move-dataset-to-container.sh e move-dataset-to-hdfs.sh
![image](https://github.com/danicosme/fiap_25abd_pd_ex3/blob/main/prints/dataset-scripts-maquina-local.png)

- Execução do script move-dataset-to-container.sh
![image](https://github.com/danicosme/fiap_25abd_pd_ex3/blob/main/prints/execucao-script-move-to-container.png)
![image](https://github.com/danicosme/fiap_25abd_pd_ex3/blob/main/prints/arquivos-container-apos-script.png)

- Execução do script move-dataset-to-hdfs.sh
![image](https://github.com/danicosme/fiap_25abd_pd_ex3/blob/main/prints/arquivos-hdfs-apos-script.png)

