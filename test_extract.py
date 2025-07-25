#!/usr/bin/env python3
"""
Script de teste para validar a extração de dados da B3 localmente
Este script testa apenas a parte de extração, sem salvar no S3
"""

import os
import sys
from datetime import datetime
import pandas as pd
from ETL.extract import B3DataExtractor

def test_extraction():
    """Testa a extração de dados da B3 sem salvar no S3"""
    
    print("=== Teste de Extração B3 IBOVESPA ===")
    print(f"Data/Hora: {datetime.now()}")
    print()
    
    # Criar extrator (não precisa de bucket real para teste)
    extractor = B3DataExtractor("test-bucket")
    
    # Testar extração
    print("1. Testando extração de dados...")
    raw_data = extractor.extract_data()
    
    if not raw_data:
        print("❌ FALHA: Não foi possível extrair dados")
        return False
    
    print(f"✅ SUCESSO: Dados extraídos")
    print(f"   - Encontrados {len(raw_data.get('results', []))} registros")
    
    # Mostrar estrutura dos dados
    if 'results' in raw_data and len(raw_data['results']) > 0:
        print(f"   - Exemplo do primeiro registro:")
        first_record = raw_data['results'][0]
        for key, value in first_record.items():
            print(f"     {key}: {value}")
    
    print()
    
    # Testar preparação do DataFrame
    print("2. Testando preparação do DataFrame...")
    df = extractor.prepare_dataframe(raw_data)
    
    if df.empty:
        print("❌ FALHA: DataFrame vazio")
        return False
    
    print(f"✅ SUCESSO: DataFrame criado")
    print(f"   - {len(df)} linhas")
    print(f"   - {len(df.columns)} colunas")
    print(f"   - Colunas: {list(df.columns)}")
    
    # Mostrar amostra dos dados
    print()
    print("3. Amostra dos dados processados:")
    print(df.head(3).to_string())
    
    # Estatísticas básicas
    print()
    print("4. Estatísticas básicas:")
    if 'part' in df.columns:
        print(f"   - Participação média: {df['part'].mean():.3f}%")
        print(f"   - Maior participação: {df['part'].max():.3f}% ({df.loc[df['part'].idxmax(), 'cod']})")
    
    if 'theoricalQty' in df.columns:
        total_qty = df['theoricalQty'].sum()
        print(f"   - Quantidade teórica total: {total_qty:,.0f}")
    
    # Salvar amostra local para verificação
    output_file = f"test_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(output_file, index=False)
    print(f"   - Dados salvos localmente em: {output_file}")
    
    print()
    print("✅ TESTE CONCLUÍDO COM SUCESSO!")
    return True

def test_data_quality(df):
    """Testa a qualidade dos dados extraídos"""
    
    print()
    print("5. Verificação de qualidade dos dados:")
    
    # Verificar campos obrigatórios
    required_fields = ['cod', 'asset']
    for field in required_fields:
        if field in df.columns:
            missing_count = df[field].isna().sum()
            print(f"   - {field}: {missing_count} valores nulos")
        else:
            print(f"   - ❌ Campo obrigatório '{field}' não encontrado")
    
    # Verificar duplicatas
    if 'cod' in df.columns:
        duplicates = df['cod'].duplicated().sum()
        print(f"   - Códigos duplicados: {duplicates}")
    
    # Verificar consistência numérica
    if 'part' in df.columns:
        invalid_parts = df[df['part'] < 0].shape[0]
        print(f"   - Participações negativas: {invalid_parts}")
        
        total_part = df['part'].sum()
        print(f"   - Soma das participações: {total_part:.3f}% (deve ser ~100%)")

if __name__ == "__main__":
    try:
        success = test_extraction()
        
        if not success:
            print("❌ Teste falhou")
            sys.exit(1)
        else:
            print("🎉 Todos os testes passaram!")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n⚠️  Teste interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erro inesperado no teste: {e}")
        sys.exit(1) 