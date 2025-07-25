#!/usr/bin/env python3
"""
ETL B3 IBOVESPA - Main Execution Script

This script orchestrates the extraction of B3 IBOVESPA data and saves it to S3.
It's designed for local execution and AWS environments.

For AWS Glue, import the B3DataExtractor class directly from ETL.extract module.

Usage:
    # Local execution
    python main.py
    
    # With custom configurations
    export B3_PAGE_SIZE=100
    export B3_EXTRACT_ALL_PAGES=true
    python main.py
"""

import logging
import os
import sys
from ETL.extract import B3DataExtractor

# Configurações
S3_BUCKET = os.getenv('S3_BUCKET', 'your-bucket-name-here')
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

def main():
    """Função principal do script"""
    try:
        logger.info("=== Iniciando ETL Extract - B3 IBOVESPA ===")
        
        # Verificar configurações
        if S3_BUCKET == 'your-bucket-name-here':
            logger.error("Configure a variável de ambiente S3_BUCKET antes de executar o script")
            return False
        
        logger.info(f"Bucket S3 configurado: {S3_BUCKET}")
        logger.info(f"Região AWS: {AWS_REGION}")
        logger.info(f"Configurações da API B3:")
        logger.info(f"  - Índice: {B3_INDEX}")
        logger.info(f"  - Tamanho da página: {B3_PAGE_SIZE}")
        logger.info(f"  - Idioma: {B3_LANGUAGE}")
        logger.info(f"  - Extrair todas as páginas: {B3_EXTRACT_ALL_PAGES}")
        
        # Instanciar extrator com configurações
        extractor = B3DataExtractor(
            s3_bucket=S3_BUCKET,
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
        
        # Salvar no S3
        success = extractor.save_to_s3(df)
        
        if success:
            logger.info("=== ETL Extract concluído com sucesso ===")
            return True
        else:
            logger.error("=== ETL Extract falhou ===")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado na função principal: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 