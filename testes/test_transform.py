#!/usr/bin/env python3
"""
Script de teste para validar as transformações de dados da B3 localmente
Este script testa apenas a parte de transformação, usando dados simulados
"""

import os
import sys
from datetime import datetime, date
import pandas as pd

# Adicionar o diretório raiz do projeto ao Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ETL.transform import B3DataTransformer

def create_sample_data():
    """Cria dados de exemplo para teste das transformações"""
    
    sample_data = [
        {
            'cod': 'ABEV3',
            'asset': 'AMBEV S/A',
            'part': 2.85,
            'theoricalQty': 1234567,
            'segment': 'N1',
            'type': 'ON',
            'partAcum': 2.85
        },
        {
            'cod': 'ABEV3',
            'asset': 'AMBEV S/A',
            'part': 1.15,
            'theoricalQty': 523456,
            'segment': 'N1', 
            'type': 'ON',
            'partAcum': 4.00
        },
        {
            'cod': 'PETR4',
            'asset': 'PETROBRAS',
            'part': 8.75,
            'theoricalQty': 987654,
            'segment': 'NM',
            'type': 'PN',
            'partAcum': 12.75
        },
        {
            'cod': 'PETR3',
            'asset': 'PETROBRAS',
            'part': 1.25,
            'theoricalQty': 234567,
            'segment': 'NM',
            'type': 'ON',
            'partAcum': 14.00
        },
        {
            'cod': 'VALE3',
            'asset': 'VALE',
            'part': 5.50,
            'theoricalQty': 1876543,
            'segment': 'NM',
            'type': 'ON',
            'partAcum': 19.50
        },
        {
            'cod': 'ITUB4',
            'asset': 'ITAU UNIBANCO',
            'part': 3.25,
            'theoricalQty': 1456789,
            'segment': 'N1',
            'type': 'PN',
            'partAcum': 22.75
        }
    ]
    
    return pd.DataFrame(sample_data)

def test_transformations():
    """Testa as transformações de dados"""
    
    print("=== Teste de Transformações B3 IBOVESPA ===")
    print(f"Data/Hora: {datetime.now()}")
    print()
    
    # Criar dados de exemplo
    print("1. Criando dados de exemplo...")
    df_original = create_sample_data()
    
    print(f"✅ Dados de exemplo criados:")
    print(f"   - {len(df_original)} registros")
    print(f"   - {len(df_original.columns)} colunas")
    print(f"   - Colunas: {list(df_original.columns)}")
    
    print()
    print("Dados originais:")
    print(df_original.to_string(index=False))
    
    print()
    
    # Criar instância do transformador (não precisa de S3 real para teste)
    transformer = B3DataTransformer("test-bucket")
    
    # Testar transformações
    print("2. Testando transformações...")
    
    try:
        df_transformed = transformer.transform_data(df_original)
        
        print(f"✅ SUCESSO: Transformações aplicadas")
        print(f"   - {len(df_transformed)} registros após transformação")
        print(f"   - {len(df_transformed.columns)} colunas")
        print(f"   - Colunas: {list(df_transformed.columns)}")
        
        print()
        print("Dados transformados:")
        print(df_transformed.to_string(index=False))
        
        return df_original, df_transformed
        
    except Exception as e:
        print(f"❌ FALHA: Erro nas transformações: {e}")
        return None, None

def validate_transformations(df_original, df_transformed):
    """Valida se as transformações foram aplicadas corretamente"""
    
    print()
    print("3. Validando transformações...")
    
    # Verificar se o DataFrame não está vazio
    if df_transformed.empty:
        print("❌ FALHA: DataFrame transformado está vazio")
        return False
    
    # Verificar colunas esperadas
    expected_columns = ['acao', 'qtd_codigo', 'participacao', 'qtd_teorica_total', 'data']
    missing_columns = [col for col in expected_columns if col not in df_transformed.columns]
    
    if missing_columns:
        print(f"❌ FALHA: Colunas obrigatórias não encontradas: {missing_columns}")
        return False
    
    print("✅ Todas as colunas obrigatórias estão presentes")
    
    # Verificar se colunas indesejadas foram removidas
    unwanted_columns = ['segment', 'type', 'partAcum']
    found_unwanted = [col for col in unwanted_columns if col in df_transformed.columns]
    
    if found_unwanted:
        print(f"❌ FALHA: Colunas indesejadas ainda presentes: {found_unwanted}")
        return False
    
    print("✅ Colunas indesejadas foram removidas corretamente")
    
    # Verificar agrupamento por asset
    assets_original = df_original['asset'].nunique()
    acoes_transformed = len(df_transformed)
    
    if assets_original != acoes_transformed:
        print(f"❌ FALHA: Número de assets não confere - Original: {assets_original}, Transformado: {acoes_transformed}")
        return False
    
    print(f"✅ Agrupamento por asset correto - {acoes_transformed} ações únicas")
    
    # Verificar agregações
    print()
    print("4. Verificando agregações:")
    
    for _, row in df_transformed.iterrows():
        acao = row['acao']
        original_subset = df_original[df_original['asset'] == acao]
        
        # Verificar contagem de códigos
        expected_qtd_codigo = len(original_subset)
        actual_qtd_codigo = row['qtd_codigo']
        
        if expected_qtd_codigo != actual_qtd_codigo:
            print(f"❌ FALHA: Quantidade de códigos para {acao} - Esperado: {expected_qtd_codigo}, Atual: {actual_qtd_codigo}")
            return False
        
        # Verificar soma de participação
        expected_participacao = original_subset['part'].sum()
        actual_participacao = row['participacao']
        
        if abs(expected_participacao - actual_participacao) > 0.01:  # tolerância de 0.01
            print(f"❌ FALHA: Participação para {acao} - Esperado: {expected_participacao}, Atual: {actual_participacao}")
            return False
        
        # Verificar soma de quantidade teórica
        expected_qtd_teorica = original_subset['theoricalQty'].sum()
        actual_qtd_teorica = row['qtd_teorica_total']
        
        if expected_qtd_teorica != actual_qtd_teorica:
            print(f"❌ FALHA: Quantidade teórica para {acao} - Esperado: {expected_qtd_teorica}, Atual: {actual_qtd_teorica}")
            return False
        
        print(f"   ✅ {acao}: qtd_codigo={actual_qtd_codigo}, participacao={actual_participacao}, qtd_teorica={actual_qtd_teorica}")
    
    # Verificar coluna de data
    expected_date = date.today().strftime('%Y-%m-%d')
    data_values = df_transformed['data'].unique()
    
    if len(data_values) != 1 or data_values[0] != expected_date:
        print(f"❌ FALHA: Data incorreta - Esperado: {expected_date}, Atual: {data_values}")
        return False
    
    print(f"✅ Coluna 'data' adicionada corretamente: {expected_date}")
    
    return True

def test_data_quality(df_transformed):
    """Testa a qualidade dos dados transformados"""
    
    print()
    print("5. Verificação de qualidade dos dados transformados:")
    
    # Verificar valores nulos
    for col in df_transformed.columns:
        null_count = df_transformed[col].isnull().sum()
        if null_count > 0:
            print(f"   ❌ {col}: {null_count} valores nulos")
        else:
            print(f"   ✅ {col}: sem valores nulos")
    
    # Verificar tipos de dados
    print()
    print("   Tipos de dados:")
    for col in df_transformed.columns:
        dtype = df_transformed[col].dtype
        print(f"   - {col}: {dtype}")
    
    # Verificar estatísticas
    print()
    print("   Estatísticas:")
    if 'participacao' in df_transformed.columns:
        total_participacao = df_transformed['participacao'].sum()
        print(f"   - Participação total: {total_participacao:.2f}%")
    
    if 'qtd_teorica_total' in df_transformed.columns:
        total_qty = df_transformed['qtd_teorica_total'].sum()
        print(f"   - Quantidade teórica total: {total_qty:,.0f}")
    
    print(f"   - Total de ações: {len(df_transformed)}")

def save_test_results(df_original, df_transformed):
    """Salva os resultados do teste localmente"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Salvar dados originais
    original_file = f"test_transform_original_{timestamp}.csv"
    df_original.to_csv(original_file, index=False)
    
    # Salvar dados transformados
    transformed_file = f"test_transform_result_{timestamp}.csv"
    df_transformed.to_csv(transformed_file, index=False)
    
    print()
    print("6. Arquivos de teste salvos:")
    print(f"   - Dados originais: {original_file}")
    print(f"   - Dados transformados: {transformed_file}")

if __name__ == "__main__":
    try:
        # Executar testes
        df_original, df_transformed = test_transformations()
        
        if df_original is None or df_transformed is None:
            print("❌ Teste falhou na fase de transformação")
            sys.exit(1)
        
        # Validar transformações
        validation_success = validate_transformations(df_original, df_transformed)
        
        if not validation_success:
            print("❌ Teste falhou na validação")
            sys.exit(1)
        
        # Testar qualidade dos dados
        test_data_quality(df_transformed)
        
        # Salvar resultados
        save_test_results(df_original, df_transformed)
        
        print()
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("✅ As transformações estão funcionando corretamente!")
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\n⚠️  Teste interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erro inesperado no teste: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 