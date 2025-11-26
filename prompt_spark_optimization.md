# Prompt Spark Optimization

Este documento contém prompts úteis para otimização de Apache Spark.

## Otimização de Performance

### Análise de Jobs Spark
```
Analise o seguinte job Spark e sugira otimizações para melhorar a performance:
- Verifique o particionamento dos dados
- Identifique possíveis shuffles desnecessários
- Sugira o uso adequado de cache/persist
- Avalie o uso de broadcast joins
```

### Tuning de Configurações
```
Revise as configurações do Spark abaixo e sugira ajustes para:
- spark.executor.memory
- spark.executor.cores
- spark.sql.shuffle.partitions
- spark.default.parallelism
```

## Otimização de Código

### Refatoração de Transformações
```
Refatore o código Spark abaixo para:
- Reduzir o número de shuffles
- Utilizar transformações narrow quando possível
- Aplicar filtros o mais cedo possível (predicate pushdown)
- Evitar collect() em datasets grandes
```

### Uso Eficiente de DataFrames
```
Otimize o seguinte código para usar DataFrames/Datasets em vez de RDDs:
- Aproveite o Catalyst Optimizer
- Utilize funções built-in do Spark SQL
- Evite UDFs quando possível
```

## Otimização de Memória

### Gerenciamento de Cache
```
Avalie a estratégia de cache/persist no código:
- Identifique dados que devem ser cacheados
- Sugira o nível de storage adequado (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
- Identifique quando fazer unpersist
```

### Serialização
```
Sugira melhorias de serialização para:
- Configuração do Kryo serializer
- Registro de classes para serialização
- Otimização de estruturas de dados
```

## Otimização de I/O

### Leitura de Dados
```
Otimize a leitura de dados considerando:
- Formato de arquivo (Parquet, ORC, Delta)
- Particionamento de dados
- Pruning de colunas e partições
- Uso de schema inference vs schema explícito
```

### Escrita de Dados
```
Melhore a escrita de dados para:
- Coalesce vs Repartition
- Compressão adequada
- Número de arquivos de saída
- Particionamento de saída
```
