## Import libraries
import boto3
import os
from dotenv import load_dotenv
from typing import List

## Load an read variables
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') 
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

## Client S3 configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_REGION
)

## Read local files Function
def ler_arquivos(pasta: str) -> List[str]:
    arquivos:List[str] = []
    for nome_arquivo in os.listdir(pasta):
        nome_completo = os.path.join(pasta,nome_arquivo)
        if os.path.isfile(nome_completo):
            arquivos.append(nome_completo)
        print('ler_arquivos')
    return arquivos

## Write on S3
def upload_s3(lista_arquivo:List[str], bucket:str) -> None:
    for arquivo in lista_arquivo:
        nome_arquivo = os.path.basename(arquivo)
        s3_client.upload_file(arquivo,bucket,nome_arquivo)
        print('upload_s3')

## Delete from local file
def deletar_arquivo_local(arquivos:List[str]) -> None:
    for arquivo in arquivos:
        os.remove(arquivo)
        print(f"{arquivo} removido com sucesso!")
## Pipeline
def executar_pipeline(path:str):
    arquivos:List[str] = ler_arquivos(path)
    if arquivos:
        try:
            upload_s3(arquivos,BUCKET_NAME)
            deletar_arquivo_local(arquivos)
        except Exception as e:
            print(f"Erro - {e}") 


# def ler_bucket():
#     response = s3_client.list_objects_v2(Bucket = BUCKET_NAME)
#     print(response['Contents'])

if __name__ == "__main__":
    PASTA_LOCAL = "files"
    executar_pipeline(PASTA_LOCAL)
    # ler_bucket()