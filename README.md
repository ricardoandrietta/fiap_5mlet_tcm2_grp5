# ETL B3 IBOVESPA - Extract Script

Este script Python extrai dados do índice IBOVESPA do site da B3 (Brasil, Bolsa, Balcão) e salva os dados em formato Parquet no Amazon S3 com particionamento diário.

## Funcionalidades

- **API REST da B3**: Acesso direto à API oficial da B3 (não web scraping)
- **URL dinâmica**: Parâmetros da API gerados automaticamente via base64
- **Configuração flexível**: Tamanho de página, índice e idioma configuráveis
- **Paginação automática**: Opção para extrair todas as páginas disponíveis
- **Formato Parquet**: Dados salvos em formato otimizado para análise
- **Particionamento diário**: Organização automática por data (year/month/day)
- **Bucket S3 configurável**: Fácil configuração via variáveis de ambiente
- **Retry logic**: Múltiplas tentativas com delays inteligentes
- **Logging detalhado**: Rastreamento completo de todas as operações

## Estrutura dos Dados

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

```bash
python extract.py
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
└── ibovespa-data/
    └── year=2024/
        └── month=01/
            └── day=15/
                └── ibovespa_20240115.parquet
```

## Campos do DataFrame Final

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

## Próximos Passos

Este é o script **extract.py** da pipeline ETL. Os próximos componentes serão:

- **transform.py**: Limpeza e transformação dos dados
- **load.py**: Carregamento final para data warehouse/analytics

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