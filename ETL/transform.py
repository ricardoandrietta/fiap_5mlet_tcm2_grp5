#!/usr/bin/env python3
"""
ETL Transform Module - B3 IBOVESPA Data Transformer

This module contains the B3DataTransformer class for transforming data from B3 
that was previously extracted and saved to S3.

Usage:
    from ETL.transform import B3DataTransformer
    
    transformer = B3DataTransformer(
        s3_bucket="my-bucket",
        aws_region="us-east-1"
    )
    
    transformer.transform_and_save()
"""

import pandas as pd
import boto3
from datetime import datetime, date
import logging
from typing import Optional
from botocore.exceptions import NoCredentialsError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Setup logging
logger = logging.getLogger(__name__)

class B3DataTransformer:
    """Classe para transformar dados do IBOVESPA extraídos da B3"""
    
    def __init__(self, s3_bucket: str, aws_region: str = "us-east-1"):
        """
        Inicializa o transformador de dados da B3
        
        Args:
            s3_bucket: Nome do bucket S3 onde estão os dados
            aws_region: Região AWS para S3
        """
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
    
    def read_from_s3(self, partition_date: date = None) -> Optional[pd.DataFrame]:
        """
        Lê dados do S3 em formato parquet
        
        Args:
            partition_date: Data da partição para ler (default: hoje)
            
        Returns:
            DataFrame pandas com os dados ou None se houver erro
        """
        try:
            if partition_date is None:
                partition_date = date.today()
            
            # Criar chave do S3 com particionamento
            year = partition_date.year
            month = f"{partition_date.month:02d}"
            day = f"{partition_date.day:02d}"
            
            s3_key = f"ibovespa-data/year={year}/month={month}/day={day}/ibovespa_{partition_date.strftime('%Y%m%d')}.parquet"
            
            logger.info(f"Lendo dados do S3: s3://{self.s3_bucket}/{s3_key}")
            
            # Download do arquivo parquet
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            buffer = BytesIO(response['Body'].read())
            
            # Carregar DataFrame
            df = pd.read_parquet(buffer, engine='pyarrow')
            
            logger.info(f"Dados carregados com sucesso: {len(df)} registros")
            return df
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"Arquivo não encontrado no S3: {s3_key}")
            else:
                logger.error(f"Erro do cliente AWS: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro ao ler dados do S3: {e}")
            return None
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica as transformações nos dados
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame transformado
        """
        try:
            logger.info("Iniciando transformações dos dados...")
            
            # Verificar se as colunas necessárias existem
            required_columns = ['asset', 'cod', 'part', 'theoricalQty']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Colunas obrigatórias não encontradas: {missing_columns}")
                raise ValueError(f"Colunas obrigatórias não encontradas: {missing_columns}")
            
            # 1. Fazer agrupamento pela coluna "asset"
            logger.info("Realizando agrupamento por 'asset'...")
            
            grouped_df = df.groupby('asset').agg({
                'cod': 'count',           # 1.1 contar a quantidade de "cod"
                'part': 'sum',            # 1.2 somar a coluna "part" 
                'theoricalQty': 'sum'     # 1.3 somar a coluna "theoricalQty"
            }).reset_index()
            
            # Renomear as colunas agregadas
            grouped_df = grouped_df.rename(columns={
                'cod': 'qtd_codigo',
                'part': 'participacao',
                'theoricalQty': 'qtd_teorica_total'
            })
            
            # 2. Renomear coluna "asset" para "acao"
            logger.info("Renomeando coluna 'asset' para 'acao'...")
            grouped_df = grouped_df.rename(columns={'asset': 'acao'})
            
            # 3. Adicionar coluna "data" com a data atual no formato Y-m-d
            logger.info("Adicionando coluna 'data'...")
            grouped_df['data'] = date.today().strftime('%Y-%m-%d')
            
            # 4. As colunas "segment", "type" e "partAcum" já foram descartadas
            # no processo de agrupamento (apenas as colunas especificadas no agg foram mantidas)
            
            logger.info(f"Transformações concluídas. {len(grouped_df)} registros processados.")
            logger.info(f"Colunas finais: {list(grouped_df.columns)}")
            
            return grouped_df
            
        except Exception as e:
            logger.error(f"Erro ao transformar dados: {e}")
            raise
    
    def save_transformed_to_s3(self, df: pd.DataFrame, partition_date: date = None) -> bool:
        """
        Salva o DataFrame transformado no S3 em formato parquet
        
        Args:
            df: DataFrame transformado para salvar
            partition_date: Data para particionamento (default: hoje)
            
        Returns:
            True se salvou com sucesso, False caso contrário
        """
        try:
            if df.empty:
                logger.warning("DataFrame vazio, não salvando no S3")
                return False
            
            if partition_date is None:
                partition_date = date.today()
            
            # Criar chave do S3 com particionamento para dados transformados
            year = partition_date.year
            month = f"{partition_date.month:02d}"
            day = f"{partition_date.day:02d}"
            
            s3_key = f"ibovespa-data-transformed/year={year}/month={month}/day={day}/ibovespa_transformed_{partition_date.strftime('%Y%m%d')}.parquet"
            
            # Converter DataFrame para parquet em memória
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            # Upload para S3 (sobrescrever se já existir)
            self.s3_client.upload_fileobj(
                buffer,
                self.s3_bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'Metadata': {
                        'source': 'b3-ibovespa-transformed',
                        'transformation_date': partition_date.isoformat(),
                        'record_count': str(len(df)),
                        'schema_version': '1.0'
                    }
                }
            )
            
            logger.info(f"Dados transformados salvos no S3: s3://{self.s3_bucket}/{s3_key}")
            logger.info(f"Total de registros salvos: {len(df)}")
            
            return True
            
        except NoCredentialsError:
            logger.error("Credenciais AWS não encontradas")
            return False
        except ClientError as e:
            logger.error(f"Erro do cliente AWS: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao salvar dados transformados no S3: {e}")
            return False
    
    def transform_and_save(self, source_date: date = None, target_date: date = None) -> bool:
        """
        Executa o processo completo de transformação: lê, transforma e salva
        
        Args:
            source_date: Data dos dados fonte (default: hoje)
            target_date: Data para salvar dados transformados (default: hoje)
            
        Returns:
            True se o processo foi executado com sucesso, False caso contrário
        """
        try:
            logger.info("Iniciando processo de transformação...")
            
            # Ler dados do S3
            df = self.read_from_s3(source_date)
            if df is None:
                logger.error("Falha ao ler dados do S3")
                return False
            
            # Aplicar transformações
            transformed_df = self.transform_data(df)
            
            # Salvar dados transformados
            success = self.save_transformed_to_s3(transformed_df, target_date)
            
            if success:
                logger.info("Processo de transformação concluído com sucesso!")
            else:
                logger.error("Falha ao salvar dados transformados")
            
            return success
            
        except Exception as e:
            logger.error(f"Erro no processo de transformação: {e}")
            return False


def main():
    """Função principal para execução do script"""
    import os
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Obter configurações do ambiente
    s3_bucket = os.getenv('S3_BUCKET', 'default-bucket')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    
    # Criar instância do transformador
    transformer = B3DataTransformer(
        s3_bucket=s3_bucket,
        aws_region=aws_region
    )
    
    # Executar transformação
    success = transformer.transform_and_save()
    
    if success:
        print("✅ Transformação executada com sucesso!")
    else:
        print("❌ Falha na execução da transformação!")
        exit(1)


if __name__ == "__main__":
    main() 