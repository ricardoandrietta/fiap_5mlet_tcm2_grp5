#!/usr/bin/env python3
"""
Script para configurar o ambiente de desenvolvimento
"""

import os
import sys
import subprocess
import platform

def check_python_version():
    """Verifica se a versão do Python é compatível"""
    print("1. Verificando versão do Python...")
    
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python {version.major}.{version.minor} não é compatível")
        print("   Requer Python 3.8 ou superior")
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
    return True

def check_pip():
    """Verifica se pip está disponível"""
    print("\n2. Verificando pip...")
    
    try:
        import pip
        print("✅ pip está disponível")
        return True
    except ImportError:
        print("❌ pip não encontrado")
        print("   Instale pip primeiro: https://pip.pypa.io/en/stable/installation/")
        return False

def create_virtual_environment():
    """Cria ambiente virtual se não existir"""
    print("\n3. Configurando ambiente virtual...")
    
    venv_path = "venv"
    
    if os.path.exists(venv_path):
        print("✅ Ambiente virtual já existe")
        return True
    
    try:
        print("   Criando ambiente virtual...")
        subprocess.run([sys.executable, "-m", "venv", venv_path], check=True)
        print("✅ Ambiente virtual criado")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao criar ambiente virtual: {e}")
        return False

def get_activation_command():
    """Retorna o comando para ativar o ambiente virtual"""
    system = platform.system().lower()
    
    if system == "windows":
        return "venv\\Scripts\\activate"
    else:
        return "source venv/bin/activate"

def install_dependencies():
    """Instala as dependências do requirements.txt"""
    print("\n4. Instalando dependências...")
    
    # Determinar o executável Python do venv
    system = platform.system().lower()
    if system == "windows":
        python_exe = "venv\\Scripts\\python.exe"
        pip_exe = "venv\\Scripts\\pip.exe"
    else:
        python_exe = "venv/bin/python"
        pip_exe = "venv/bin/pip"
    
    if not os.path.exists(python_exe):
        print("❌ Ambiente virtual não encontrado")
        return False
    
    try:
        # Atualizar pip
        print("   Atualizando pip...")
        subprocess.run([python_exe, "-m", "pip", "install", "--upgrade", "pip"], 
                      check=True, capture_output=True)
        
        # Instalar dependências
        print("   Instalando dependências do requirements.txt...")
        subprocess.run([pip_exe, "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        
        print("✅ Dependências instaladas com sucesso")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao instalar dependências: {e}")
        return False

def create_env_example():
    """Cria arquivo .env.example se não existir"""
    print("\n5. Configurando arquivo de exemplo de variáveis de ambiente...")
    
    env_example_content = """# Arquivo de exemplo para configuração das variáveis de ambiente
# Copie este arquivo para .env e configure com seus valores reais

# OBRIGATÓRIO: Nome do bucket S3 onde os dados serão salvos
S3_BUCKET=your-bucket-name-here

# OPCIONAL: Região AWS (default: us-east-1)
AWS_REGION=us-east-1

# OPCIONAL: Nível de log (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO

# OPCIONAL: Credenciais AWS (recomendado usar AWS CLI ou IAM Role)
# AWS_ACCESS_KEY_ID=your-access-key-id
# AWS_SECRET_ACCESS_KEY=your-secret-access-key

# OPCIONAL: Configurações de timeout e retry
# REQUEST_TIMEOUT=30
# MAX_RETRIES=3"""

    if not os.path.exists(".env.example"):
        with open(".env.example", "w") as f:
            f.write(env_example_content)
        print("✅ Arquivo .env.example criado")
    else:
        print("✅ Arquivo .env.example já existe")
    
    return True

def check_aws_cli():
    """Verifica se AWS CLI está instalado"""
    print("\n6. Verificando AWS CLI...")
    
    try:
        result = subprocess.run(["aws", "--version"], 
                              capture_output=True, text=True, check=True)
        print(f"✅ AWS CLI encontrado: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️  AWS CLI não encontrado")
        print("   Para instalar: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
        return False

def main():
    """Função principal do setup"""
    print("=== Configuração do Ambiente - ETL B3 IBOVESPA ===")
    print()
    
    # Verificações básicas
    if not check_python_version():
        return False
    
    if not check_pip():
        return False
    
    # Configurar ambiente virtual
    if not create_virtual_environment():
        return False
    
    # Instalar dependências
    if not install_dependencies():
        return False
    
    # Criar arquivos de configuração
    if not create_env_example():
        return False
    
    # Verificar AWS CLI (opcional)
    check_aws_cli()
    
    # Instruções finais
    print("\n" + "="*50)
    print("🎉 AMBIENTE CONFIGURADO COM SUCESSO!")
    print("="*50)
    
    activation_cmd = get_activation_command()
    
    print("\n📋 PRÓXIMOS PASSOS:")
    print(f"1. Ative o ambiente virtual:")
    print(f"   {activation_cmd}")
    print()
    print("2. Configure suas credenciais AWS:")
    print("   aws configure")
    print("   (ou configure as variáveis de ambiente)")
    print()
    print("3. Configure o bucket S3:")
    print("   export S3_BUCKET='seu-bucket-s3'")
    print("   (ou edite o arquivo .env)")
    print()
    print("4. Teste a extração localmente:")
    print("   python test_extract.py")
    print()
    print("5. Execute a extração completa:")
    print("   python extract.py")
    print()
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Setup interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro inesperado no setup: {e}")
        sys.exit(1) 