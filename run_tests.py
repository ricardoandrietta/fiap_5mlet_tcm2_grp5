#!/usr/bin/env python3
"""
Script para executar todos os testes da Pipeline ETL B3 IBOVESPA

Este script executa todos os testes disponíveis e fornece um relatório consolidado.

Usage:
    # Executar todos os testes
    python run_tests.py
    
    # Executar apenas teste de extração
    python run_tests.py extract
    
    # Executar apenas teste de transformação
    python run_tests.py transform
"""

import sys
import os
import subprocess
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_test(test_name, test_file):
    """
    Executa um teste específico
    
    Args:
        test_name: Nome do teste para exibição
        test_file: Caminho para o arquivo de teste
    
    Returns:
        bool: True se o teste passou, False caso contrário
    """
    try:
        logger.info(f"Executando {test_name}...")
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {test_name} - PASSOU")
            return True
        else:
            logger.error(f"❌ {test_name} - FALHOU")
            logger.error(f"Erro: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {test_name} - TIMEOUT (mais de 5 minutos)")
        return False
    except Exception as e:
        logger.error(f"❌ {test_name} - ERRO: {e}")
        return False

def run_extract_test():
    """Executa teste de extração"""
    return run_test("Teste de Extração", "testes/test_extract.py")

def run_transform_test():
    """Executa teste de transformação"""
    return run_test("Teste de Transformação", "testes/test_transform.py")

def main():
    """Função principal"""
    print("=" * 60)
    print("PIPELINE ETL B3 IBOVESPA - SUITE DE TESTES")
    print("=" * 60)
    print(f"Data/Hora: {datetime.now()}")
    print()
    
    # Determinar quais testes executar
    test_type = 'all'
    if len(sys.argv) > 1:
        test_type = sys.argv[1].lower()
    
    valid_types = ['all', 'extract', 'transform']
    if test_type not in valid_types:
        print(f"❌ Tipo de teste inválido: {test_type}")
        print(f"Tipos válidos: {', '.join(valid_types)}")
        return False
    
    # Lista de testes a executar
    tests_to_run = []
    
    if test_type in ['all', 'extract']:
        tests_to_run.append(('Teste de Extração', run_extract_test))
    
    if test_type in ['all', 'transform']:
        tests_to_run.append(('Teste de Transformação', run_transform_test))
    
    # Executar testes
    passed = 0
    total = len(tests_to_run)
    
    for test_name, test_func in tests_to_run:
        if test_func():
            passed += 1
    
    # Relatório final
    print("\n" + "=" * 60)
    print("RELATÓRIO FINAL")
    print("=" * 60)
    
    if passed == total:
        print(f"🎉 TODOS OS TESTES PASSARAM! ({passed}/{total})")
        print("✅ A pipeline ETL está funcionando corretamente!")
        return True
    else:
        print(f"❌ ALGUNS TESTES FALHARAM! ({passed}/{total})")
        print("⚠️  Verifique os logs acima para detalhes dos erros.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Testes interrompidos pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        sys.exit(1) 