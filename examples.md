# Exemplos de Uso - ETL B3 IBOVESPA

## Configurações da API B3

### Exemplo 1: Configuração Padrão (IBOVESPA Completo)

```bash
# Usar configurações padrão
python extract.py
```

**Parâmetros gerados:**
- pageSize: 1200
- index: IBOV
- language: pt-br
- páginas: apenas primeira

**URL da API:**
```
https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJwYWdlTnVtYmVyIjoxLCJwYWdlU2l6ZSI6MTIwMCwibGFuZ3VhZ2UiOiJwdC1iciIsImluZGV4IjoiSUJPViJ9
```

### Exemplo 2: Extração com Páginas Menores

```bash
# Configurar para 100 registros por página
export B3_PAGE_SIZE=100
python extract.py
```

**Resultado:** 
- Primeira página com 100 registros
- URL com parâmetro: `{"pageNumber":1,"pageSize":100,"language":"pt-br","index":"IBOV"}`

### Exemplo 3: Extrair Todas as Páginas

```bash
# Extrair todas as páginas disponíveis
export B3_PAGE_SIZE=50
export B3_EXTRACT_ALL_PAGES=true
python extract.py
```

**Comportamento:**
- Primeira requisição: descobre total de páginas
- Requisições subsequentes: extrai páginas 2, 3, 4...
- Consolidação: todos os registros em um único arquivo

### Exemplo 4: Índice Diferente (Small Caps)

```bash
# Configurar para Small Caps
export B3_INDEX=SMLL
export B3_PAGE_SIZE=500
python extract.py
```

**URL gerada:**
```
{"pageNumber":1,"pageSize":500,"language":"pt-br","index":"SMLL"}
```

### Exemplo 5: Resposta em Inglês

```bash
# API em inglês
export B3_LANGUAGE=en-us
python extract.py
```

### Exemplo 6: Configuração Completa para Produção

```bash
#!/bin/bash
# Script de produção com todas as configurações

# AWS
export S3_BUCKET="meu-bucket-producao"
export AWS_REGION="us-east-1"

# B3 API
export B3_PAGE_SIZE=1200        # Máximo por página
export B3_INDEX=IBOV           # Ibovespa
export B3_LANGUAGE=pt-br       # Português
export B3_EXTRACT_ALL_PAGES=false  # Apenas primeira página

# Logging
export LOG_LEVEL=INFO

# Executar
python extract.py
```

## Estruturas de Arquivos Geradas

### Arquivo único (B3_EXTRACT_ALL_PAGES=false)

```
s3://meu-bucket/
└── ibovespa-data/
    └── year=2025/
        └── month=07/
            └── day=25/
                └── ibovespa_20250725.parquet  # ~84 registros
```

### Múltiplas páginas consolidadas (B3_EXTRACT_ALL_PAGES=true)

```
s3://meu-bucket/
└── ibovespa-data/
    └── year=2025/
        └── month=07/
            └── day=25/
                └── ibovespa_20250725.parquet  # Todos os registros
```

## Logs de Exemplo

### Configuração Padrão
```
2025-07-25 14:32:40 - INFO - === Iniciando ETL Extract - B3 IBOVESPA ===
2025-07-25 14:32:40 - INFO - Bucket S3 configurado: meu-bucket
2025-07-25 14:32:40 - INFO - Região AWS: us-east-1
2025-07-25 14:32:40 - INFO - Configurações da API B3:
2025-07-25 14:32:40 - INFO -   - Índice: IBOV
2025-07-25 14:32:40 - INFO -   - Tamanho da página: 1200
2025-07-25 14:32:40 - INFO -   - Idioma: pt-br
2025-07-25 14:32:40 - INFO -   - Extrair todas as páginas: False
2025-07-25 14:32:40 - INFO - Extraindo apenas a primeira página...
2025-07-25 14:32:42 - INFO - ✅ Dados extraídos com sucesso! 84 registros encontrados.
```

### Múltiplas Páginas
```
2025-07-25 14:32:40 - INFO - Extraindo todas as páginas disponíveis...
2025-07-25 14:32:42 - INFO - Encontradas 3 páginas. Extraindo páginas restantes...
2025-07-25 14:32:45 - INFO - Página 2: +50 registros
2025-07-25 14:32:48 - INFO - Página 3: +34 registros
2025-07-25 14:32:48 - INFO - Extração completa: 134 registros de 3 páginas
```

## Testes Locais

### Teste com Parâmetros Customizados

```bash
# Testar com configuração específica
export B3_PAGE_SIZE=50
export B3_INDEX=IBOV
python test_extract.py
```

### Verificar URL Gerada

```python
from extract import B3DataExtractor

# Criar extrator com parâmetros específicos
extractor = B3DataExtractor(
    s3_bucket="test",
    page_size=100,
    index="SMLL",
    language="en-us"
)

# Ver URL gerada
url = extractor._build_api_url(page_number=2)
print(url)
```

## Uso no Spark

```bash
# Executar no EMR/Spark com configurações
spark-submit \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.env.B3_PAGE_SIZE=1200 \
  --conf spark.env.B3_EXTRACT_ALL_PAGES=true \
  --packages org.apache.spark:spark-sql_2.12:3.4.0 \
  extract.py
``` 