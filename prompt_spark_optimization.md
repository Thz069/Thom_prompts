# PROMPT: OTIMIZAÇÃO E DIAGNÓSTICO DE AWS GLUE (SPARK)

**Persona:** Aja como um Engenheiro de Dados Principal e Especialista em FinOps de AWS. Você tem conhecimento profundo da arquitetura interna do Apache Spark (Catalyst Optimizer, Gerenciamento de Memória Tungsten, Shuffle Service) e da infraestrutura do AWS Glue.

**Objetivo:** Analise os dados técnicos, o código e, principalmente, as EVIDÊNCIAS DE LOGS E MÉTRICAS fornecidas abaixo para diagnosticar a causa raiz da ineficiência ou falha do job.

---

## 1. CONTEXTO TÉCNICO DO JOB

* **Objetivo do Job:** [Ex: Ingestão de dados de vendas do ERP para o Data Lake]
* **Versão do Glue:** [Ex: Glue 4.0 (Spark 3.3)]
* **Tipo de Worker:** [Ex: G.1X / G.2X / Z.2X]
* **Número de Workers:** [Ex: 50 fixos OU Auto Scaling habilitado (Min 2, Max 50)]
* **Recursos Ativos:** [Ex: Job Bookmarks, Glue Flex]
* **Perfil dos Dados (Origem):**
    * Formato: [Ex: JSON / Parquet / CSV]
    * Volume Total: [Ex: 500 GB]
    * Estrutura de Arquivos: [Ex: Milhares de arquivos pequenos (~100KB cada) no S3]
* **Perfil dos Dados (Destino):**
    * Formato: [Ex: Parquet Snappy]
    * Particionamento: [Ex: Particionado por `ano/mes/dia`]

---

## 2. EVIDÊNCIAS DE EXECUÇÃO (Onde está o problema)

**A. Logs de Erro (CloudWatch / S3 stderr):**
*(Cole aqui o Stack Trace do erro. Procure por "Caused by", "OutOfMemoryError", "ExecutorLostFailure")*
> [COLE O ERRO AQUI]

**B. Análise do Spark UI (Performance):**
*(Dados extraídos do Spark History Server ou Event Logs do S3)*
* **Estágio Gargalo (ID e Duração):** [Ex: Stage 8 demorou 1h (90% do tempo total)]
* **Operação do Estágio:** [Ex: SortMergeJoin / GroupBy]
* **Shuffle Read/Write:** [Ex: O estágio gravou 200GB de Shuffle]
* **Data Skew (Desbalanceamento):** [Ex: A tarefa mediana leva 2min, mas a tarefa MÁXIMA leva 45min]
* **GC Time (Garbage Collection):** [Ex: Baixo (<5%) OU Alto (>15% indicando pressão de memória)]

**C. Métricas de Infraestrutura (CloudWatch Metrics):**
* **Uso de Memória (Heap Usage):** [Ex: Heap dos executores atinge 90% e falha]
* **Uso de CPU:** [Ex: CPU fica baixa (<10%) a maior parte do tempo (possível gargalo de Driver ou I/O)]
* **Disco:** [Ex: Houve "Spill to Disk" detectado?]

---

## 3. SNIPPET DE CÓDIGO (LÓGICA CRÍTICA)

*(Abaixo está o trecho do código PySpark correspondente ao estágio lento ou onde ocorre o erro)*

```python
# [COLE O TRECHO RELEVANTE DO SEU SCRIPT AQUI]
# Ex:
# df_final = df_a.join(df_b, "id", "left").groupBy("regiao").agg(sum("valor"))
# df_final.write.mode("overwrite").parquet("s3://...")
4. SOLICITAÇÃO DE OTIMIZAÇÃO
Com base nos logs e contexto acima, forneça:

Diagnóstico da Causa Raiz: Explique tecnicamente o motivo da falha ou lentidão (Ex: Data Skew, Driver OOM, Small Files).

Solução de Código (Refatoração): Reescreva o snippet acima aplicando as correções necessárias (Ex: Salting, Coalesce, Broadcast).

Tuning de Spark (--conf): Quais parâmetros específicos devo configurar? (Ex: spark.sql.shuffle.partitions, spark.memory.fraction).

Recomendação de Infra: Devo alterar o tipo de worker (G.1X vs G.2X) ou a quantidade?
