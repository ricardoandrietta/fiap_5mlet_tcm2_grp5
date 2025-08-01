#!/usr/bin/env python3
"""
ETL B3 IBOVESPA - Main Execution Script

This script orchestrates the extraction and transformation of B3 IBOVESPA data and saves it to S3.
It's designed for local execution and AWS environments.

For AWS Glue, import the B3DataExtractor and B3DataTransformer classes directly from ETL modules.

Usage:
    # Extract data only
    python main.py extract
    
    # Transform data only
    python main.py transform
    
    # Run both extract and transform (pipeline)
    python main.py pipeline
    
    # Default: extract only
    python main.py
    
    # With custom configurations
    export B3_PAGE_SIZE=100
    export B3_EXTRACT_ALL_PAGES=true
    python main.py extract
"""

import logging
import os
import sys
from datetime import date
from ETL.extract import B3DataExtractor
from ETL.transform import B3DataTransformer

# Configurações
STORAGE_TYPE = os.getenv('STORAGE_TYPE', 's3')  # 's3' ou 'local'
S3_BUCKET = os.getenv('S3_BUCKET', 'your-bucket-name-here')
LOCAL_ROOT = os.getenv('LOCAL_ROOT', 'extracted_raw')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Configurações da API B3
B3_PAGE_SIZE = int(os.getenv('B3_PAGE_SIZE', '1200'))
B3_INDEX = os.getenv('B3_INDEX', 'IBOV')
B3_LANGUAGE = os.getenv('B3_LANGUAGE', 'pt-br')
B3_EXTRACT_ALL_PAGES = os.getenv('B3_EXTRACT_ALL_PAGES', 'false').lower() == 'true'

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_data():
    """Executa apenas a extração de dados"""
    try:
        logger.info("=== Iniciando ETL Extract - B3 IBOVESPA ===")
        
        # Instanciar extrator com configurações
        extractor = B3DataExtractor(
            storage_type=STORAGE_TYPE,
            s3_bucket=S3_BUCKET if STORAGE_TYPE == 's3' else None,
            local_root=LOCAL_ROOT if STORAGE_TYPE == 'local' else None,
            page_size=B3_PAGE_SIZE,
            index=B3_INDEX,
            language=B3_LANGUAGE,
            aws_region=AWS_REGION
        )
        
        # Extrair dados (uma página ou todas)
        if B3_EXTRACT_ALL_PAGES:
            logger.info("Extraindo todas as páginas disponíveis...")
            raw_data = extractor.extract_all_pages()
        else:
            logger.info("Extraindo apenas a primeira página...")
            raw_data = extractor.extract_data()
        
        if not raw_data:
            logger.error("Falha na extração de dados")
            return False
        
        # Preparar DataFrame
        df = extractor.prepare_dataframe(raw_data)
        
        if df.empty:
            logger.error("DataFrame vazio após processamento")
            return False
        
        # Salvar dados
        success = extractor.save_data(df)
        
        if success:
            logger.info("=== ETL Extract concluído com sucesso ===")
            return True
        else:
            logger.error("=== ETL Extract falhou ===")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado na extração: {e}")
        return False

def transform_data():
    """Executa apenas a transformação de dados"""
    try:
        logger.info("=== Iniciando ETL Transform - B3 IBOVESPA ===")
        
        # Instanciar transformador
        transformer = B3DataTransformer(
            s3_bucket=S3_BUCKET,
            aws_region=AWS_REGION
        )
        
        # Executar transformação
        success = transformer.transform_and_save()
        
        if success:
            logger.info("=== ETL Transform concluído com sucesso ===")
            return True
        else:
            logger.error("=== ETL Transform falhou ===")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado na transformação: {e}")
        return False

def run_pipeline():
    """Executa o pipeline completo: extração seguida de transformação"""
    try:
        logger.info("=== Iniciando Pipeline ETL Completo - B3 IBOVESPA ===")
        
        # 1. Extração
        extract_success = extract_data()
        
        if not extract_success:
            logger.error("Pipeline falhou na etapa de extração")
            return False
        
        # 2. Transformação
        transform_success = transform_data()
        
        if not transform_success:
            logger.error("Pipeline falhou na etapa de transformação")
            return False
        
        logger.info("=== Pipeline ETL Completo concluído com sucesso ===")
        return True
        
    except Exception as e:
        logger.error(f"Erro inesperado no pipeline: {e}")
        return False

def main():
    """Função principal do script"""
    try:
        # Verificar configurações
        if STORAGE_TYPE == 's3' and S3_BUCKET == 'your-bucket-name-here':
            logger.error("Configure a variável de ambiente S3_BUCKET antes de executar o script")
            return False
        
        logger.info(f"Tipo de armazenamento: {STORAGE_TYPE}")
        if STORAGE_TYPE == 's3':
            logger.info(f"Bucket S3 configurado: {S3_BUCKET}")
            logger.info(f"Região AWS: {AWS_REGION}")
        else:
            logger.info(f"Pasta local configurada: {LOCAL_ROOT}")
        
        # Determinar operação baseada nos argumentos
        operation = 'extract'  # default
        if len(sys.argv) > 1:
            operation = sys.argv[1].lower()
        
        valid_operations = ['extract', 'transform', 'pipeline']
        if operation not in valid_operations:
            logger.error(f"Operação inválida: {operation}")
            logger.error(f"Operações válidas: {', '.join(valid_operations)}")
            return False
        
        # Mostrar configurações para extração
        if operation in ['extract', 'pipeline']:
            logger.info(f"Configurações da API B3:")
            logger.info(f"  - Índice: {B3_INDEX}")
            logger.info(f"  - Tamanho da página: {B3_PAGE_SIZE}")
            logger.info(f"  - Idioma: {B3_LANGUAGE}")
            logger.info(f"  - Extrair todas as páginas: {B3_EXTRACT_ALL_PAGES}")
        
        # Executar operação solicitada
        if operation == 'extract':
            return extract_data()
        elif operation == 'transform':
            return transform_data()
        elif operation == 'pipeline':
            return run_pipeline()
        
        return False
            
    except Exception as e:
        logger.error(f"Erro inesperado na função principal: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 