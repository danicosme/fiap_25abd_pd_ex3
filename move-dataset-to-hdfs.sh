#Criando pastas no HDFS
hadoop fs -rm -r /datasets/youtube
hadoop fs -mkdir /datasets/youtube

#Transferindo arquivos para o HDFS
hadoop fs -put /datasets/BR_category_id.json          /datasets/youtube/BR_category_id.json                      
hadoop fs -put /datasets/BR_youtube_trending_data.csv /datasets/youtube/BR_youtube_trending_data.csv     
hadoop fs -put /datasets/CA_category_id.json          /datasets/youtube/CA_category_id.json                      
hadoop fs -put /datasets/CA_youtube_trending_data.csv /datasets/youtube/CA_youtube_trending_data.csv     
hadoop fs -put /datasets/DE_category_id.json          /datasets/youtube/DE_category_id.json                      
hadoop fs -put /datasets/DE_youtube_trending_data.csv /datasets/youtube/DE_youtube_trending_data.csv     
hadoop fs -put /datasets/FR_category_id.json          /datasets/youtube/FR_category_id.json                      
hadoop fs -put /datasets/FR_youtube_trending_data.csv /datasets/youtube/FR_youtube_trending_data.csv     
hadoop fs -put /datasets/GB_category_id.json          /datasets/youtube/GB_category_id.json                      
hadoop fs -put /datasets/GB_youtube_trending_data.csv /datasets/youtube/GB_youtube_trending_data.csv     
hadoop fs -put /datasets/IN_category_id.json          /datasets/youtube/IN_category_id.json                      
hadoop fs -put /datasets/IN_youtube_trending_data.csv /datasets/youtube/IN_youtube_trending_data.csv     
hadoop fs -put /datasets/JP_category_id.json          /datasets/youtube/JP_category_id.json                      
hadoop fs -put /datasets/JP_youtube_trending_data.csv /datasets/youtube/JP_youtube_trending_data.csv     
hadoop fs -put /datasets/KR_category_id.json          /datasets/youtube/KR_category_id.json                      
hadoop fs -put /datasets/KR_youtube_trending_data.csv /datasets/youtube/KR_youtube_trending_data.csv     
hadoop fs -put /datasets/MX_category_id.json          /datasets/youtube/MX_category_id.json                      
hadoop fs -put /datasets/MX_youtube_trending_data.csv /datasets/youtube/MX_youtube_trending_data.csv     
hadoop fs -put /datasets/RU_category_id.json          /datasets/youtube/RU_category_id.json                      
hadoop fs -put /datasets/RU_youtube_trending_data.csv /datasets/youtube/RU_youtube_trending_data.csv     
hadoop fs -put /datasets/US_category_id.json          /datasets/youtube/US_category_id.json                      
hadoop fs -put /datasets/US_youtube_trending_data.csv /datasets/youtube/US_youtube_trending_data.csv     