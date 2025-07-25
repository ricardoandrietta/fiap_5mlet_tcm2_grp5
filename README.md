# ETL B3 IBOVESPA - Pipeline Completo

Este projeto contém uma pipeline ETL completa para processar dados do índice IBOVESPA da B3 (Brasil, Bolsa, Balcão). A pipeline inclui extração, transformação e salvamento dos dados em formato Parquet no Amazon S3 com particionamento diário.

## Funcionalidades

### Extract (ETL/extract.py)
- **API REST da B3**: Acesso direto à API oficial da B3 (não web scraping)
- **URL dinâmica**: Parâmetros da API gerados automaticamente via base64
- **Configuração flexível**: Tamanho de página, índice e idioma configuráveis
- **Paginação automática**: Opção para extrair todas as páginas disponíveis
- **Retry logic**: Múltiplas tentativas com delays inteligentes

### Transform (ETL/transform.py)
- **Agregação por ativo**: Agrupamento automático por asset
- **Cálculos de métricas**: Contagem de códigos, soma de participação e quantidade teórica
- **Limpeza de dados**: Remoção de colunas desnecessárias
- **Enriquecimento**: Adição de timestamp de processamento
- **Padronização**: Renomeação de colunas conforme especificação

### Geral
- **Formato Parquet**: Dados salvos em formato otimizado para análise
- **Particionamento diário**: Organização automática por data (year/month/day)
- **Bucket S3 configurável**: Fácil configuração via variáveis de ambiente
- **Logging detalhado**: Rastreamento completo de todas as operações
- **Pipeline flexível**: Execute extract, transform ou pipeline completo

## Estrutura dos Dados

### Dados Extraídos (Raw)
O script extrai os dados da seção "results" que contém:

```json
{
  "segment": null,
  "cod": "ALOS3",
  "asset": "ALLOS", 
  "type": "ON  ED  NM",
  "part": "0,490",
  "partAcum": null,
  "theoricalQty": "476.976.044"
}
```

### Dados Transformados
Após a transformação, os dados são agregados por ativo:

```json
{
  "acao": "ALLOS",
  "qtd_codigo": 2,
  "participacao": 0.98,
  "qtd_teorica_total": 953952088,
  "data": "2025-07-25"
}
```

### Transformações Aplicadas
1. **Agrupamento por asset**: Dados agrupados pela coluna "asset"
2. **Agregações**:
   - `qtd_codigo`: Contagem de códigos por ativo
   - `participacao`: Soma da participação percentual
   - `qtd_teorica_total`: Soma da quantidade teórica
3. **Renomeação**: Coluna "asset" renomeada para "acao"
4. **Enriquecimento**: Adição da coluna "data" com data de processamento
5. **Limpeza**: Remoção das colunas "segment", "type" e "partAcum"

## Instalação

1. Clone este repositório
2. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração

### Variáveis de Ambiente

Configure as seguintes variáveis de ambiente:

```bash
# Configurações obrigatórias
export S3_BUCKET="seu-bucket-s3"

# Configurações opcionais da AWS
export AWS_REGION="us-east-1"  # default: us-east-1
export LOG_LEVEL="INFO"        # default: INFO

# Configurações opcionais da API B3
export B3_PAGE_SIZE="1200"     # default: 1200 registros por página
export B3_INDEX="IBOV"         # default: IBOV (Ibovespa)
export B3_LANGUAGE="pt-br"     # default: pt-br
export B3_EXTRACT_ALL_PAGES="false"  # default: false (apenas primeira página)
```

### Credenciais AWS

Configure suas credenciais AWS usando um dos métodos:

1. **AWS CLI**:
```bash
aws configure
```

2. **Variáveis de ambiente**:
```bash
export AWS_ACCESS_KEY_ID="sua-access-key"
export AWS_SECRET_ACCESS_KEY="sua-secret-key"
```

3. **IAM Role** (recomendado para EC2/Lambda)

### Permissões S3 Necessárias

O usuário/role deve ter as seguintes permissões no bucket S3:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::seu-bucket/*"
        }
    ]
}
```

## Parâmetros da API B3

O script gera dinamicamente URLs da API B3 com base nos parâmetros configurados:

| Parâmetro | Descrição | Valores Possíveis | Default |
|-----------|-----------|-------------------|---------|
| `B3_PAGE_SIZE` | Registros por página | 1-1200 | 1200 |
| `B3_INDEX` | Índice da B3 | IBOV, SMLL, MLCX, etc. | IBOV |
| `B3_LANGUAGE` | Idioma da resposta | pt-br, en-us | pt-br |
| `B3_EXTRACT_ALL_PAGES` | Extrair todas as páginas | true, false | false |

### Exemplos de URLs Geradas

```bash
# Configuração padrão (1200 registros do IBOVESPA)
https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJwYWdlTnVtYmVyIjoxLCJwYWdlU2l6ZSI6MTIwMCwibGFuZ3VhZ2UiOiJwdC1iciIsImluZGV4IjoiSUJPViJ9

# Com 100 registros por página
export B3_PAGE_SIZE="100"
# Gerará URL com: {"pageNumber":1,"pageSize":100,"language":"pt-br","index":"IBOV"}
```

## Uso

### Execução Local

O script principal (`main.py`) oferece três operações:

```bash
# Apenas extração (default)
python main.py extract
# ou simplesmente
python main.py

# Apenas transformação
python main.py transform

# Pipeline completo (extração + transformação)
python main.py pipeline
```

### Execução de Módulos Individuais

```bash
# Executar apenas extração
python ETL/extract.py

# Executar apenas transformação
python ETL/transform.py
```

### Execução de Testes

```bash
# Executar todos os testes
python run_tests.py

# Executar apenas teste de extração
python run_tests.py extract

# Executar apenas teste de transformação
python run_tests.py transform

# Executar testes individuais
python testes/test_extract.py
python testes/test_transform.py
```

### Uso no AWS Spark/EMR

1. Faça upload do script para seu cluster Spark
2. Configure as variáveis de ambiente
3. Execute com spark-submit:

```bash
spark-submit --packages org.apache.spark:spark-sql_2.12:3.4.0 extract.py
```

### Uso no AWS Lambda

1. Crie um package com as dependências
2. Configure as variáveis de ambiente na função Lambda
3. Configure timeout adequado (recomendado: 5+ minutos)

## Estrutura de Arquivos no S3

Os dados são salvos com a seguinte estrutura:

```
s3://seu-bucket/
├── ibovespa-data/                    # Dados brutos extraídos
│   └── year=2024/
│       └── month=01/
│           └── day=15/
│               └── ibovespa_20240115.parquet
└── ibovespa-data-transformed/        # Dados transformados
    └── year=2024/
        └── month=01/
            └── day=15/
                └── ibovespa_transformed_20240115.parquet
```

## Campos dos DataFrames

### DataFrame Extraído (Raw)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| segment | string | Segmento (geralmente null) |
| cod | string | Código do ativo (ex: ALOS3) |
| asset | string | Nome do ativo |
| type | string | Tipo do ativo |
| part | float | Participação percentual |
| partAcum | string | Participação acumulada |
| theoricalQty | float | Quantidade teórica |
| extraction_date | date | Data da extração |
| extraction_timestamp | datetime | Timestamp da extração |
| data_date | string | Data dos dados (do header) |
| total_theoretical_qty | string | Quantidade teórica total |
| reductor | string | Redutor |

### DataFrame Transformado

| Campo | Tipo | Descrição |
|-------|------|-----------|
| acao | string | Nome do ativo (renomeado de asset) |
| qtd_codigo | int | Quantidade de códigos por ativo |
| participacao | float | Soma da participação percentual por ativo |
| qtd_teorica_total | int | Soma da quantidade teórica por ativo |
| data | string | Data de processamento (Y-m-d) |

## Logs

O script gera logs detalhados incluindo:

- Início e fim da extração
- Número de registros encontrados
- Sucesso/falha do upload para S3
- Detalhes de erros quando ocorrem

## Tratamento de Erros

O script é robusto e trata:

- Falhas de conexão HTTP
- Timeout de requisições
- Diferentes formatos de resposta (JSON/HTML)
- Falhas de autenticação AWS
- Erros de upload S3
- Dados malformados

## Componentes da Pipeline

A pipeline ETL B3 IBOVESPA contém os seguintes componentes:

- ✅ **extract.py**: Extração de dados da API B3 (Implementado)
- ✅ **transform.py**: Transformação e agregação dos dados (Implementado)
- 🔄 **load.py**: Carregamento final para data warehouse/analytics (Próximo passo)

### Scripts de Teste
- ✅ **testes/test_extract.py**: Teste local da extração
- ✅ **testes/test_transform.py**: Teste local das transformações
- ✅ **run_tests.py**: Script para executar todos os testes

## Troubleshooting

### Erro: "Configure a variável de ambiente S3_BUCKET"
- Defina a variável de ambiente S3_BUCKET com o nome do seu bucket

### Erro: "Credenciais AWS não encontradas"
- Configure suas credenciais AWS conforme descrito na seção de configuração

### Erro: "Nenhum dado encontrado na resposta"
- Verifique se o site da B3 está acessível
- O site pode ter mudado sua estrutura (requer atualização do script)

### Dados não aparecem no S3
- Verifique as permissões do bucket S3
- Confirme se a região AWS está correta
- Verifique os logs para detalhes do erro 