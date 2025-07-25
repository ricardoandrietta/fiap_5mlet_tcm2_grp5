#!/usr/bin/env python3
"""
ETL Extract Module - B3 IBOVESPA Data Extractor

This module contains the B3DataExtractor class for extracting data from B3 API
and saving it to S3 in Parquet format with daily partitioning.

Usage:
    from ETL.extract import B3DataExtractor
    
    extractor = B3DataExtractor(
        s3_bucket="my-bucket",
        page_size=1200,
        index="IBOV",
        language="pt-br"
    )
    
    data = extractor.extract_data()
    df = extractor.prepare_dataframe(data)
    extractor.save_to_s3(df)
"""

import requests
import pandas as pd
import boto3
from datetime import datetime, date
import json
import logging
import os
import time
import random
import base64
from typing import Dict, List, Optional
from botocore.exceptions import NoCredentialsError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Setup logging
logger = logging.getLogger(__name__)

class B3DataExtractor:
    """Classe para extrair dados do IBOVESPA da B3"""
    
    def __init__(self, s3_bucket: str, page_size: int = 1200, index: str = "IBOV", 
                 language: str = "pt-br", aws_region: str = "us-east-1"):
        """
        Inicializa o extrator de dados da B3
        
        Args:
            s3_bucket: Nome do bucket S3 para salvar os dados
            page_size: Número de registros por página (1-1200)
            index: Índice da B3 (IBOV, SMLL, MLCX, etc.)
            language: Idioma da API (pt-br, en-us)
            aws_region: Região AWS para S3
        """
        self.base_api_url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay"
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Parâmetros configuráveis da API
        self.page_size = page_size
        self.index = index
        self.language = language
        
        # Headers para simular um navegador real
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://sistemaswebb3-listados.b3.com.br/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    
    def _build_api_url(self, page_number: int = 1) -> str:
        """
        Constrói a URL da API com parâmetros base64 dinâmicos
        
        Args:
            page_number: Número da página (default: 1)
        
        Returns:
            URL completa com parâmetros base64 codificados
        """
        # Parâmetros da API
        params = {
            "pageNumber": page_number,
            "pageSize": self.page_size,
            "language": self.language,
            "index": self.index
        }
        
        # Converter para JSON e depois para base64
        json_params = json.dumps(params, separators=(',', ':'))
        base64_params = base64.b64encode(json_params.encode('utf-8')).decode('utf-8')
        
        # Construir URL completa
        url = f"{self.base_api_url}/{base64_params}"
        
        logger.debug(f"Parâmetros da API: {params}")
        logger.debug(f"URL construída: {url}")
        
        return url
    
    def extract_data(self, max_retries: int = 3) -> Optional[Dict]:
        """
        Extrai dados da API da B3 com estratégia de retry
        
        Args:
            max_retries: Número máximo de tentativas
        
        Returns:
            Dict com os dados extraídos ou None se houver erro
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Tentativa {attempt + 1}/{max_retries} - Extraindo dados da B3...")
                
                # Delay aleatório para evitar rate limiting
                if attempt > 0:
                    delay = random.uniform(2, 5)
                    logger.info(f"Aguardando {delay:.1f} segundos antes da próxima tentativa...")
                    time.sleep(delay)
                
                # Construir URL dinâmica e fazer a requisição
                api_url = self._build_api_url()
                response = requests.get(
                    api_url,
                    headers=self.headers,
                    timeout=30
                )
                
                logger.info(f"Status da resposta: {response.status_code}")
                logger.info(f"Content-Type: {response.headers.get('content-type', 'não informado')}")
                
                response.raise_for_status()
                
                # Parse direto do JSON
                data = response.json()
                
                if data and 'results' in data:
                    logger.info(f"✅ Dados extraídos com sucesso! {len(data['results'])} registros encontrados.")
                    return data
                else:
                    logger.warning("⚠️ Nenhum dado 'results' encontrado na resposta da API")
                    logger.info(f"Chaves disponíveis: {list(data.keys()) if data else 'Nenhuma'}")
                    if data:  # Se tem dados, mesmo sem 'results', retorna
                        return data
                    
            except requests.RequestException as e:
                logger.error(f"❌ Erro na requisição HTTP (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error("Todas as tentativas falharam")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"❌ Erro ao fazer parse do JSON (tentativa {attempt + 1}): {e}")
                # Para erro de JSON, mostrar início da resposta
                try:
                    logger.info(f"Início da resposta: {response.text[:200]}...")
                except:
                    pass
                if attempt == max_retries - 1:
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Erro inesperado na extração (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return None
        
        return None
    
    def extract_all_pages(self, max_retries: int = 3) -> Optional[Dict]:
        """
        Extrai todas as páginas da API automaticamente
        
        Args:
            max_retries: Número máximo de tentativas por página
        
        Returns:
            Dict com todos os dados consolidados ou None se houver erro
        """
        logger.info("Iniciando extração de todas as páginas...")
        
        # Extrair primeira página para obter informações de paginação
        first_page = self.extract_data(max_retries)
        if not first_page:
            logger.error("Falha ao extrair primeira página")
            return None
        
        page_info = first_page.get('page', {})
        total_pages = page_info.get('totalPages', 1)
        
        if total_pages <= 1:
            logger.info("Apenas uma página disponível")
            return first_page
        
        logger.info(f"Encontradas {total_pages} páginas. Extraindo páginas restantes...")
        
        # Consolidar todos os resultados
        all_results = first_page['results'].copy()
        
        # Extrair páginas restantes
        for page_num in range(2, total_pages + 1):
            logger.info(f"Extraindo página {page_num}/{total_pages}...")
            
            # Pequeno delay entre páginas
            time.sleep(random.uniform(1, 2))
            
            page_data = self._extract_single_page(page_num, max_retries)
            if page_data and 'results' in page_data:
                all_results.extend(page_data['results'])
                logger.info(f"Página {page_num}: +{len(page_data['results'])} registros")
            else:
                logger.warning(f"Falha ao extrair página {page_num}")
        
        # Atualizar dados consolidados
        consolidated_data = first_page.copy()
        consolidated_data['results'] = all_results
        consolidated_data['page']['totalRecords'] = len(all_results)
        
        logger.info(f"Extração completa: {len(all_results)} registros de {total_pages} páginas")
        return consolidated_data
    
    def _extract_single_page(self, page_number: int, max_retries: int = 3) -> Optional[Dict]:
        """
        Extrai uma página específica
        
        Args:
            page_number: Número da página
            max_retries: Número máximo de tentativas
        
        Returns:
            Dict com os dados da página ou None se houver erro
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = random.uniform(1, 3)
                    time.sleep(delay)
                
                api_url = self._build_api_url(page_number)
                response = requests.get(api_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                return data
                
            except Exception as e:
                logger.error(f"Erro ao extrair página {page_number} (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return None
        
        return None
    
    def prepare_dataframe(self, raw_data: Dict) -> pd.DataFrame:
        """
        Prepara o DataFrame com os dados extraídos
        
        Args:
            raw_data: Dados brutos da extração
            
        Returns:
            DataFrame pandas com os dados processados
        """
        try:
            results = raw_data.get('results', [])
            
            if not results:
                logger.warning("Nenhum resultado encontrado nos dados")
                return pd.DataFrame()
            
            # Criar DataFrame
            df = pd.DataFrame(results)
            
            # Adicionar metadados importantes
            df['extraction_date'] = datetime.now().date()
            df['extraction_timestamp'] = datetime.now()
            
            # Adicionar informações do header se disponível
            header = raw_data.get('header', {})
            if header:
                df['data_date'] = header.get('date')
                df['total_theoretical_qty'] = header.get('theoricalQty')
                df['reductor'] = header.get('reductor')
            
            # Limpar e converter tipos de dados
            df = self._clean_dataframe(df)
            
            logger.info(f"DataFrame criado com {len(df)} registros")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao preparar DataFrame: {e}")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e converte tipos de dados do DataFrame
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame limpo
        """
        try:
            # Converter colunas numéricas
            numeric_columns = ['part', 'theoricalQty']
            
            for col in numeric_columns:
                if col in df.columns:
                    # Remover caracteres não numéricos exceto vírgula e ponto
                    df[col] = df[col].astype(str).str.replace(r'[^\d,.-]', '', regex=True)
                    # Converter vírgula para ponto (formato brasileiro para americano)
                    df[col] = df[col].str.replace(',', '.')
                    # Converter para float
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Limpar strings
            string_columns = ['cod', 'asset', 'type', 'segment']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace('None', None)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao limpar DataFrame: {e}")
            return df
    
    def save_to_s3(self, df: pd.DataFrame, partition_date: date = None) -> bool:
        """
        Salva o DataFrame no S3 em formato parquet com particionamento diário
        
        Args:
            df: DataFrame para salvar
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
            
            # Criar chave do S3 com particionamento
            year = partition_date.year
            month = f"{partition_date.month:02d}"
            day = f"{partition_date.day:02d}"
            
            s3_key = f"ibovespa-data/year={year}/month={month}/day={day}/ibovespa_{partition_date.strftime('%Y%m%d')}.parquet"
            
            # Converter DataFrame para parquet em memória
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            # Upload para S3
            self.s3_client.upload_fileobj(
                buffer,
                self.s3_bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'Metadata': {
                        'source': 'b3-ibovespa',
                        'extraction_date': partition_date.isoformat(),
                        'record_count': str(len(df))
                    }
                }
            )
            
            logger.info(f"Dados salvos no S3: s3://{self.s3_bucket}/{s3_key}")
            logger.info(f"Total de registros salvos: {len(df)}")
            
            return True
            
        except NoCredentialsError:
            logger.error("Credenciais AWS não encontradas")
            return False
        except ClientError as e:
            logger.error(f"Erro do cliente AWS: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao salvar no S3: {e}")
            return False 