# ✈️ Projeto Aviação ANAC — VoeBem Analytics

Projeto desenvolvido durante a **Imersão Engenharia de Dados com IA — setembro/2026**, utilizando **Databricks, SQL, PySpark e dados da ANAC**.

A ideia foi acompanhar, na prática, o fluxo dos dados desde a camada Bronze até a Gold, passando por transformação, qualidade, governança e análise com IA.

---

## 📌 Status dos desafios

| Desafio                                     | Status      | Resultado                                   |
| ------------------------------------------- | ----------- | ------------------------------------------- |
| **Desafio 1 — Validando dados da Bronze**   | ✅ Concluído | Dados ingeridos e validados                 |
| **Desafio 2 — Comparando Bronze x Silver**  | ✅ Concluído | Transformações comparadas e validadas       |
| **Desafio 3 — Perguntas de negócio com IA** | ✅ Concluído | Perguntas realizadas e respostas analisadas |

---

# 🥉 Desafio 1 — Validando dados da camada Bronze

O primeiro desafio foi validar os dados depois da ingestão, sem modificar a tabela.

### O que foi feito

* Preparação do ambiente no Databricks
* Upload dos **12 arquivos CSV do VRA**
* Execução da ingestão para a camada Bronze
* Criação da tabela `voebem.bronze.vra`
* Validação da quantidade de registros
* Validação das colunas disponíveis
* Visualização dos primeiros registros

### 🔎 Resultado

* **Registros:** 1.014.705
* **Colunas:** 14
* **Colunas de dados:** 12
* **Colunas de auditoria:** `_arquivo_origem` e `_ingerido_em`
* **Arquivos ingeridos:** 12 CSVs

### 🧪 Consultas utilizadas

```sql
SELECT COUNT(*) AS quantidade_registros
FROM voebem.bronze.vra;

DESCRIBE voebem.bronze.vra;

SELECT *
FROM voebem.bronze.vra
LIMIT 10;
```

### Conclusão

A ingestão foi realizada corretamente e os dados ficaram disponíveis na camada Bronze para as próximas etapas do pipeline.

**Status: ✅ Concluído**

---

# 🥈 Desafio 2 — Comparando dados antes e depois da transformação

Nesta etapa foi feita a comparação entre:

```text
voebem.bronze.vra
        ↓
voebem.silver.vra
```

O objetivo foi entender quais transformações foram aplicadas aos dados.

### 🔎 Principais transformações observadas

Na Bronze, os dados foram mantidos próximos do formato original. Na Silver, foram aplicadas transformações para tornar os dados mais organizados e adequados para análise.

Entre as mudanças observadas:

* Tipificação dos campos de data e hora
* Padronização e organização dos dados
* Tratamento de campos
* Criação de colunas derivadas
* Criação de métricas de atraso
* Separação entre data e hora
* Unificação dos cadastros de empresas nacionais e estrangeiras
* Inclusão de documentação e governança

### Algumas colunas transformadas ou criadas

```text
partida_prevista
partida_prevista_data
partida_prevista_hora
atraso_partida_min
atraso_chegada_min
minutos_recuperados
```

### Validação da quantidade de registros

```text
Bronze VRA: 1.014.705
Silver VRA: 1.014.705
Diferença: 0
```

A comparação mostrou que a transformação para Silver manteve a mesma quantidade de registros da VRA.

### Tabelas de referência

Também foram criadas e validadas:

```text
voebem.silver.empresas
voebem.silver.aerodromos
voebem.silver.codigos_operacao
```

### Conclusão

A comparação mostrou que a Silver não é apenas uma cópia da Bronze. Os dados foram tipados, organizados, tratados e documentados para facilitar o uso nas próximas etapas.

**Status: ✅ Concluído**

---

# 🔎 Qualidade dos dados

Também foi configurado e executado o pipeline de qualidade para aplicar regras de validação sobre a camada Silver.

As expectations verificaram, entre outros pontos:

* presença dos horários previstos;
* situações de voo válidas;
* coerência entre horários previstos e reais;
* valores plausíveis de atraso;
* existência das empresas nos cadastros;
* existência dos aeroportos de origem e destino.

As regras foram configuradas para medir a qualidade sem descartar os registros da Silver.

---

# 🥇 Gold — Modelagem para consumo

Na camada Gold foram criadas:

```text
voebem.gold.dim_aeroporto
voebem.gold.fato_voos
voebem.gold.obt_voos
```

A execução seguiu a ordem:

```text
01_dim_aeroporto.sql
        ↓
02_fato_voos.sql
        ↓
03_obt_voos.sql
```

A OBT foi preparada como tabela de consumo para facilitar consultas e análises de negócio.

---

# 📝 Governança da Gold

Na etapa de governança foram aplicados comentários e tags nas tabelas Gold.

### Documentação das colunas

* `gold.obt_voos` → **39 colunas documentadas**
* `gold.fato_voos` → **31 colunas documentadas**
* `gold.dim_aeroporto` → **7 colunas documentadas**
* **100% das colunas documentadas**

### Tags

Foram aplicadas tags de governança nas tabelas Gold, incluindo informações como:

```text
camada
dominio
fonte
grao
```

A documentação foi pensada também para o consumo por IA, ajudando o agente a interpretar melhor o significado das tabelas e colunas.

---

# 🧠 Revisão das descrições com os dados

Durante a etapa de governança, foi feita uma revisão das descrições geradas para algumas métricas.

Um dos casos mais interessantes foi `minutos_recuperados`.

A descrição inicial sugeria que um valor positivo significava que o voo havia chegado adiantado.

Ao comparar a métrica com os dados de chegada, foi possível verificar que isso não era necessariamente verdade.

### Resultado encontrado

* **164.895 voos** recuperaram tempo durante a etapa
* **75.082 voos** recuperaram tempo e ainda chegaram com mais de 15 minutos de atraso

Isso mostrou que:

> **Recuperar tempo não significa necessariamente chegar no horário.**

A métrica mostra a recuperação do atraso entre a partida e a chegada. A pontualidade é analisada por outra métrica.

Essa validação reforçou a importância de conferir as descrições geradas pela IA diretamente nos dados.

---

# 📅 Outras validações realizadas

### Voos cancelados

Foi verificado que:

```text
29.140 de 29.140
```

voos cancelados possuem `partida_pontual = NULL`.

### `mes_referencia`

Também foi identificado que:

```text
30.798 voos
```

não possuem `mes_referencia`, pois não possuem horário previsto.

Essas verificações ajudaram a evitar interpretações incorretas das métricas.

---

# 🔗 Lineage

O Unity Catalog registrou automaticamente o lineage dos dados.

O fluxo ficou representado como:

```text
Arquivo CSV da ANAC
        ↓
voebem.bronze.vra
        ↓
voebem.silver.vra
        ↓
voebem.gold.fato_voos
        ↓
voebem.gold.obt_voos
```

Isso permite acompanhar a origem dos dados e entender o impacto de alterações ao longo do pipeline.

---

# 🤖 Desafio 3 — Fazendo perguntas de negócio com IA

Na terceira etapa, utilizei os dados preparados na Gold para realizar perguntas de negócio com IA.

O objetivo foi testar se o agente conseguia interpretar as perguntas e retornar respostas coerentes a partir dos dados preparados.

Foram realizadas **pelo menos três perguntas de negócio diferentes**, explorando informações relacionadas a:

* companhias aéreas;
* atrasos;
* aeroportos;
* rotas;
* comportamento dos dados.

Durante a análise das respostas, observei:

* clareza da resposta;
* coerência com os dados;
* interpretação da pergunta;
* necessidade de reformulação;
* relação entre a qualidade dos dados e a qualidade da resposta.

Também foi possível perceber que perguntas mais específicas ajudam a obter respostas mais claras.

### 💡 Principal aprendizado

Uma boa consulta com IA não começa apenas com o prompt.

Ela começa muito antes:

```text
dados bem preparados
        ↓
estrutura organizada
        ↓
documentação
        ↓
governança
        ↓
contexto
        ↓
pergunta
        ↓
resposta da IA
```

O resultado mostrou, na prática, como a preparação dos dados influencia diretamente a qualidade das respostas obtidas com IA.

**Status: ✅ Concluído**

---

# 📊 Resumo dos resultados

| Indicador                                      |           Resultado |
| ---------------------------------------------- | ------------------: |
| CSVs VRA ingeridos                             |                  12 |
| Bronze VRA                                     | 1.014.705 registros |
| Silver VRA                                     | 1.014.705 registros |
| Diferença Bronze x Silver                      |                   0 |
| Aeródromos Bronze                              |                 496 |
| Empresas nacionais Bronze                      |                 729 |
| Empresas estrangeiras Bronze                   |                 148 |
| Códigos de operação                            |                  13 |
| Colunas documentadas em `obt_voos`             |                  39 |
| Colunas documentadas em `fato_voos`            |                  31 |
| Colunas documentadas em `dim_aeroporto`        |                   7 |
| Cobertura da documentação                      |                100% |
| Voos que recuperaram tempo                     |             164.895 |
| Recuperaram tempo e chegaram >15 min atrasados |              75.082 |
| Voos cancelados com `partida_pontual = NULL`   |              29.140 |
| Voos com `mes_referencia = NULL`               |              30.798 |

---

# ✅ Conclusão final

Até aqui, consegui acompanhar na prática o fluxo completo de dados no Databricks, desde a ingestão na Bronze até o consumo na Gold e as consultas de negócio com IA.

O projeto mostrou na prática que cada camada tem uma responsabilidade diferente:

* **Bronze:** preservar os dados próximos da origem;
* **Silver:** organizar, tipar, tratar e documentar;
* **Gold:** preparar os dados para consumo e análise;
* **Governança:** documentar, contextualizar e rastrear os dados;
* **IA:** transformar esses dados preparados em respostas para perguntas de negócio.

Outro aprendizado importante foi perceber que a IA não deve ser tratada como dona da verdade. Algumas descrições pareciam corretas à primeira vista, mas a análise dos próprios dados mostrou outra coisa.

No final, ficou claro para mim que **dados bem preparados + boa documentação + perguntas bem formuladas** fazem muita diferença na hora de trabalhar com IA.

## 🚀 Desafios

* **Desafio 1:** ✅ Concluído
* **Desafio 2:** ✅ Concluído
* **Desafio 3:** ✅ Concluído

**Projeto concluído até a etapa de perguntas de negócio com IA.**
