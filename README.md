# MVP - Pipeline de Dados: Segurança Operacional

Pipeline Lakehouse no Databricks para organizar e analisar ocorrências de segurança entre 2020 e 2026, com rastreabilidade, qualidade e privacidade.

Autor: Cirisley Ferreira de Moraes
Matrícula: 4052026000477
Data: Setembro/2026

## 1. Contexto de Negócios e Perguntas
### 1.1 Problema
A base operacional de acidentes, quase acidentes, desvios críticos e fatalidades precisa ser anonimizada, padronizada e modelada antes de apoiar decisões preventivas.
### 1.2 Objetivo
Construir um pipeline ponta a ponta em arquitetura Medalhão, transformando registros históricos em tabelas Delta analíticas.
### 1.3 Perguntas de negócio
1. Como os eventos evoluíram? 
2. Quais segmentos concentram riscos Alto/Crítico? 
3. Quais Regras de Ouro são mais frequentes? 
4. O perfil difere entre próprios e terceiros? 
5. Quais problemas de qualidade afetam a análise?
### 1.4 Fonte dos dados
Aba `Acidentes` da planilha empresarial, com 1.989 registros de 2020 a 2026. O pipeline utiliza uma cópia que foi anonimizada com 17 campos necessários.
### 1.5 Licença
A base é interna e não é dado aberto. O uso deve ser acadêmico e autorizado. Sem autorização externa, substituir por dados sintéticos ou públicos.
### 1.6 Estrutura dos dados brutos
Campos mantidos: vínculo, classificação, empresa, segmento, idade, data, dias perdidos/debitados, agente, lesão, parte do corpo, gravidade, potencial, risco e Regra de Ouro. Nome, ID, CAT, contrato, fornecedor, líder e descrição livre foram removidos.

## 2. Carga dos Dados
### 2.1 Processo de ingestão
`Planilha → anonimização → CSV → Volume → Bronze Delta`. O `01_bronze.py` inclui `_ingestion_ts` e `_source_file`.

> **Screenshot 1:** inserir a tela do Volume com o CSV.
### 2.2 Armazenamento no Databricks
Catálogo `mvp_seguranca`, schemas `bronze`, `silver` e `gold`, e Volume `bronze.arquivos`.

> **Screenshot 2:** inserir Catalog Explorer com catálogo, schemas e Volume.

## 3. Modelagem e Catálogo de Dados
### 3.1 Arquitetura Medalhão
Bronze preserva; Silver limpa e tipa; Gold agrega para análise.
### 3.2 Modelo de dados
|Camada|Tabela|Finalidade|
|---|---|---|
|Bronze|`acidentes_raw`|Ingestão rastreável|
|Silver|`acidentes_limpos`|Base tratada|
|Gold|`eventos_por_ano`|Evolução anual|
|Gold|`risco_por_segmento`|Segmento e risco|
|Gold|`regras_de_ouro`|Ranking preventivo|
|Gold|`proprios_terceiros`|Perfil por vínculo|
### 3.3 Catálogo de Dados
O catálogo de dados da camada Silver foi estruturado com o objetivo de transformar uma base operacional heterogênea em um conjunto de informações padronizadas, confiáveis e adequadas para análises gerenciais e estratégicas de segurança. A definição dos atributos considerou não apenas a disponibilidade dos dados na origem, mas principalmente sua relevância para responder às perguntas de negócio propostas no MVP.

A camada Silver contém informações relacionadas ao vínculo do trabalhador (próprio ou terceiro), classificação da ocorrência, empresa, segmento operacional, idade, data do evento, dias perdidos e debitados, gravidade, grau de risco, agente causador, tipo de lesão, parte do corpo atingida e Compromissos/Regras de Ouro, além dos metadados de ingestão (_ingestion_ts e _source_file) e dos atributos derivados 'data_evento' e 'tipo_evento'.

A escolha desses atributos foi motivada pela necessidade de apoiar diferentes níveis de decisão dentro da gestão de segurança:

Campos temporais (ano, mês, dia e data_evento) permitem identificar tendências, sazonalidades e evolução dos indicadores ao longo do tempo.
Empresa e segmento possibilitam identificar áreas com maior concentração de registros e direcionar ações preventivas de forma mais eficiente.
Empregado (Próprio/Terceiro) permite avaliar a distribuição dos eventos entre diferentes grupos de trabalhadores, apoiando estratégias de gestão de contratadas e programas de capacitação.
Classificação, gravidade e grau de risco fornecem uma visão sobre a criticidade dos eventos registrados, auxiliando a priorização de ações corretivas e preventivas.
Agente causador, tipo de lesão e parte do corpo atingida permitem futuras análises causais e epidemiológicas, contribuindo para a definição de campanhas de prevenção e melhorias nos controles operacionais.
Compromissos/Regras de Ouro possibilitam identificar quais temas de segurança aparecem com maior frequência nos registros, apoiando a tomada de decisão sobre treinamentos, campanhas de conscientização e revisão de procedimentos.

Além dos atributos oriundos da fonte, foram criados os campos derivados data_evento e tipo_evento. A criação de data_evento elimina a necessidade de reconstrução da data em consultas analíticas futuras, aumentando a eficiência do processamento e reduzindo erros. Já tipo_evento consolida diferentes classificações operacionais em grupos analíticos mais simples, permitindo comparações e visualizações gerenciais mais intuitivas.

Os metadados _ingestion_ts e _source_file foram mantidos para garantir rastreabilidade e governança dos dados, princípios fundamentais em arquiteturas Lakehouse. Esses campos permitem identificar quando e a partir de qual arquivo cada registro foi carregado, facilitando auditorias, reprocessamentos e investigações sobre possíveis inconsistências.

Sob a perspectiva de governança, o catálogo da Silver representa o ponto de equilíbrio entre o dado bruto e o dado analítico. Enquanto a camada Bronze preserva os registros originais, a Silver entrega um conjunto consistente e padronizado que reduz ambiguidades e aumenta a confiabilidade das análises. Dessa forma, as tabelas Gold podem ser construídas com menor complexidade e maior qualidade, fornecendo informações mais robustas para suporte à decisão.

Em termos acadêmicos, essa modelagem segue os princípios da arquitetura Medalhão, nos quais a camada Silver atua como responsável pela padronização semântica, controle de qualidade e preparação dos dados para consumo analítico, contribuindo para que as decisões de segurança sejam baseadas em informações consistentes, reproduzíveis e auditáveis. Essa abordagem é especialmente relevante em ambientes corporativos do setor elétrico, onde decisões relacionadas à prevenção de acidentes, gestão de riscos e proteção dos trabalhadores dependem diretamente da confiabilidade dos dados utilizados.
> **Screenshot 3:** inserir estrutura de `silver.acidentes_limpos`.

## 4. Pipeline de Dados
### 4.1 Bronze
A camada Bronze representa o primeiro estágio da arquitetura Medalhão e tem como principal objetivo garantir a preservação integral dos dados de origem, assegurando rastreabilidade, auditoria e reprodutibilidade do processo analítico. Nessa etapa, o arquivo acidentes_mvp_anonimizado.csv é ingerido no ambiente Databricks e persistido na tabela Delta bronze.acidentes_raw, sem aplicação de regras de negócio, filtros ou transformações analíticas.

A decisão de manter os registros da forma mais próxima possível da origem está alinhada às boas práticas de Data Engineering e Governança de Dados. Ao preservar os dados originais, a organização reduz o risco de perda de informação durante o processo de transformação e mantém uma referência confiável para validação de inconsistências identificadas nas etapas posteriores do pipeline.

Outro aspecto importante da camada Bronze é a separação entre ingestão e tratamento dos dados. Essa segregação permite que as transformações realizadas na Silver possam ser revisadas, reproduzidas ou corrigidas sem a necessidade de novo carregamento da fonte original. Em ambientes corporativos, essa prática aumenta a confiabilidade dos processos analíticos e facilita auditorias internas.

Durante a ingestão são adicionados os metadados:

_ingestion_ts: registra o momento exato em que o dado foi carregado no ambiente;
_source_file: identifica o arquivo utilizado na carga.

Esses metadados possuem papel fundamental na governança do pipeline, pois permitem rastrear a origem das informações, controlar versões dos dados carregados e identificar possíveis problemas de carga ou atualização. Em cenários futuros de ingestão incremental, esses atributos também poderão apoiar mecanismos de monitoração e controle da qualidade das cargas.

Sob a perspectiva da tomada de decisão, embora a Bronze não seja utilizada diretamente para análises gerenciais, ela constitui a base de confiança sobre a qual todo o pipeline é construído. Qualquer indicador produzido nas camadas Silver ou Gold pode ser rastreado até seu dado de origem, garantindo transparência e confiabilidade dos resultados apresentados aos gestores.

Além disso, a adoção do formato Delta Lake já na camada Bronze oferece benefícios relevantes para ambientes analíticos modernos, como armazenamento otimizado, versionamento dos dados, capacidade de auditoria e melhor desempenho de leitura e escrita. Esses recursos tornam a solução mais escalável e aderente às práticas recomendadas em arquiteturas Lakehouse.

A camada Bronze desempenha o papel de fonte única da verdade dentro do pipeline. Sua função não é produzir informação analítica, mas assegurar que os registros originais permaneçam íntegros e disponíveis para processamento futuro. Essa abordagem reduz a propagação de erros, aumenta a confiabilidade dos resultados e fortalece a governança dos dados ao longo de todo o ciclo analítico. Leitura do CSV e persistência em `bronze.acidentes_raw` sem alteração analítica.

> **Screenshot 4:** inserir `display()` da Bronze.
### 4.2 Silver
Aplica `snake_case`, trim, normalização de grafias, tipagem, data, macroclassificação e deduplicação exata.

> **Screenshot 5:** inserir tabela Silver persistida.
### 4.3 Gold
A camada Gold representa o estágio de consumo analítico da arquitetura Medalhão e tem como finalidade transformar os dados já padronizados e validados da camada Silver em informações de alto valor para suporte à tomada de decisão. Nessa etapa, o objetivo deixa de ser a preparação dos dados e passa a ser a geração de conhecimento orientado ao negócio.

Para atender às perguntas definidas no escopo do MVP, foram criadas quatro tabelas analíticas:

'eventos_por_ano': evolução histórica das ocorrências por ano e tipo de evento;
'risco_por_segmento': distribuição dos eventos por segmento e grau de risco;
'regras_de_ouro': frequência dos Compromissos e Regras de Ouro associados às ocorrências;
'proprios_terceiros': comparação da distribuição dos registros entre empregados próprios e terceiros.

A decisão de estruturar tabelas específicas para cada necessidade analítica foi tomada com o objetivo de reduzir a complexidade das consultas e aumentar a facilidade de interpretação pelos usuários finais. Em vez de exigir que analistas ou gestores realizem múltiplas agregações sobre tabelas detalhadas, a camada Gold disponibiliza visões já consolidadas e alinhadas às perguntas estratégicas definidas no projeto.

Sob a perspectiva de gestão de segurança, essa abordagem permite transformar dados operacionais em indicadores capazes de direcionar ações preventivas. A tabela eventos_por_ano, por exemplo, possibilita identificar tendências de crescimento ou redução dos registros ao longo do tempo, servindo como ponto de partida para investigações sobre mudanças nos processos operacionais, cultura de reporte ou exposição ao risco.

A tabela risco_por_segmento foi criada com o propósito de apoiar a priorização de recursos e iniciativas de prevenção. Segmentos que concentram maior quantidade de ocorrências classificadas como Alto ou Crítico podem demandar revisões de procedimentos, treinamentos específicos, reforço de supervisão ou implementação de novos controles operacionais. Dessa forma, a análise deixa de ser baseada em percepção e passa a ser suportada por evidências quantitativas.

Já a tabela 'regras_de_ouro' foi concebida para identificar quais temas de segurança aparecem com maior frequência nos registros históricos. A partir dessa informação, torna-se possível direcionar campanhas educativas, programas de treinamento e revisões normativas para os assuntos que apresentam maior recorrência na organização. Em um ambiente corporativo, essa priorização contribui para uma utilização mais eficiente dos recursos destinados à prevenção.

A tabela 'proprios_terceiros' busca fornecer uma visão sobre a distribuição dos eventos entre diferentes grupos de trabalhadores. Embora a análise não permita concluir sobre taxas de acidentes devido à ausência de dados de exposição (HHT), ela oferece uma visão inicial sobre a composição dos registros e apoia futuras investigações relacionadas à gestão de contratadas, capacitação e cultura de segurança.

A camada Gold foi projetada para responder diretamente às questões de negócio levantadas na fase de entendimento do problema. Essa decisão está alinhada aos princípios de Business Intelligence e Analytics, nos quais os modelos analíticos devem ser construídos para atender necessidades específicas dos tomadores de decisão e não apenas refletir a estrutura operacional dos dados.

Além disso, a criação de tabelas agregadas melhora significativamente o desempenho das consultas e das visualizações. Como os cálculos mais complexos já foram realizados previamente, dashboards e relatórios podem acessar informações resumidas com menor tempo de processamento, aumentando a experiência do usuário e reduzindo o consumo computacional do ambiente.

A camada Gold materializa a transformação de dados em informação gerencial. Enquanto a Bronze preserva a origem e a Silver garante qualidade e padronização, a Gold entrega conhecimento estruturado para apoiar decisões. Esse conceito é consistente com o modelo DIKW (Data, Information, Knowledge and Wisdom), no qual os dados brutos passam por sucessivas etapas de tratamento até se tornarem insumos capazes de orientar ações estratégicas.

> **Screenshot 6:** inserir tabelas Gold no catálogo.
### 4.4 Transformações
Limpeza textual, conversão de tipos, formação da data, macroclassificação e agregação. Nulos clínicos/causais não são inventados.

## 5. Qualidade de Dados
### 5.1 Completude
|Campo|Ausentes|Percentual|
|---|---:|---:|
|Gravidade|669|33,6%|
|Agente causador|447|22,5%|
|Tipo de lesão|451|22,7%|
|Parte do corpo|774|38,9%|
|Latitude inválida/ausente|238|12,0%|
|Longitude inválida/ausente|237|11,9%|
### 5.2 Consistência
Há variações de maiúsculas, acentuação, grafia, espaços e separadores, tratadas na Silver.
### 5.3 Unicidade
Duplicatas exatas são removidas. Sem chave técnica anonimizada, registros apenas semelhantes são preservados.
### 5.4 Acurácia
O pipeline valida domínios e tipos, mas não comprova a correção semântica do preenchimento original.
### 5.5 Outliers
A idade varia de 18 a 76 anos, dentro da faixa de validação de 14 a 90. Dias elevados exigem avaliação de negócio antes de serem tratados como erro.
### 5.6 Tratamentos realizados
Trim, normalização, tipagem, dias nulos como zero, data, macroclassificação, padronização das Regras de Ouro e deduplicação.

> **Screenshot 7:** inserir resultado de `04_qualidade.sql`.

## 6. Análise de Dados
### 6.1 Pergunta 1
A análise temporal dos registros demonstra um crescimento consistente do volume de ocorrências registradas ao longo do período estudado. Foram identificados 48 registros em 2020, 91 em 2021, 164 em 2022, 338 em 2023, 485 em 2024, 644 em 2025 e 219 registros em 2026, sendo este último um ano ainda incompleto no momento da extração dos dados.

Sob uma perspectiva gerencial, o crescimento observado não pode ser interpretado de forma simplista como uma deterioração do desempenho em segurança. Em sistemas de gestão maduros, o aumento do número de registros pode refletir não apenas a ocorrência de acidentes, mas também uma maior capacidade organizacional de identificar, comunicar e registrar condições de risco, desvios e quase acidentes. Dessa forma, a evolução do volume de registros deve ser analisada considerando o contexto operacional e a evolução da cultura de segurança da organização.

Um aspecto relevante identificado na base é o crescimento expressivo dos registros classificados como quase acidentes ao longo do período. Esse comportamento sugere uma possível evolução da cultura de reporte, na qual os trabalhadores passam a comunicar não apenas eventos com consequências, mas também situações com potencial de causar danos. Sob a ótica da gestão preventiva, essa mudança representa um avanço importante, pois permite atuar sobre os riscos antes da materialização de acidentes mais graves.

A decisão de analisar inicialmente o comportamento histórico dos registros está diretamente associada à necessidade de compreender a evolução do sistema de segurança ao longo do tempo. A dimensão temporal permite identificar tendências, mudanças de comportamento organizacional e possíveis impactos de programas corporativos de prevenção, treinamentos, campanhas de conscientização ou alterações operacionais ocorridas durante o período analisado.

Do ponto de vista da tomada de decisão, essa informação permite aos gestores:

Avaliar a evolução da maturidade do sistema de reporte de segurança;
Identificar períodos de crescimento ou redução significativa das ocorrências;
Direcionar investigações para anos com alterações relevantes no perfil dos registros;
Priorizar ações preventivas em temas que apresentem crescimento recorrente;
Monitorar a efetividade de programas de segurança implementados ao longo do tempo.

Entretanto, é importante destacar uma limitação metodológica relevante. O volume absoluto de registros não permite, isoladamente, medir o desempenho da segurança operacional. Uma organização pode apresentar aumento no número de ocorrências registradas e, ao mesmo tempo, estar evoluindo positivamente em sua cultura de segurança caso haja maior transparência e incentivo ao reporte. Da mesma forma, uma redução de registros não necessariamente representa melhoria, podendo indicar subnotificação.

Por esse motivo, a literatura de Segurança do Trabalho e Gestão de Riscos recomenda que análises temporais sejam complementadas por indicadores normalizados de exposição, como Horas-Homem Trabalhadas (HHT), possibilitando o cálculo de métricas como Taxa de Frequência (TF), Taxa de Frequência com Afastamento (TFCA) e Taxa de Gravidade (TG). Sem esses denominadores, os resultados deste MVP devem ser interpretados como uma análise de distribuição e comportamento dos registros, e não como uma medida definitiva de desempenho em segurança.

Sob a perspectiva acadêmica, os resultados obtidos evidenciam o valor da camada Gold na transformação de dados operacionais em informações gerenciais. A tabela eventos_por_ano permite visualizar rapidamente tendências históricas e apoiar decisões estratégicas relacionadas à gestão de riscos, demonstrando como a arquitetura Medalhão contribui para converter dados brutos em conhecimento útil para a prevenção de acidentes e a melhoria contínua da segurança operacional.

![Evolução anual](evidencias/eventos_por_ano.png)

O crescimento não deve ser interpretado automaticamente como piora, pois pode incluir maior maturidade de reporte.

### 6.2 Pergunta 2
A análise inicial da base demonstra que o segmento de Transmissão concentra o maior volume histórico de registros, totalizando 704 ocorrências, seguido pelo CSC (387), Expansão (290) e Geração Hidráulica (253). Entretanto, a tomada de decisão em segurança não deve ser baseada apenas no volume absoluto de registros, mas principalmente na criticidade dos eventos e na exposição operacional associada a cada atividade.A tabela `risco_por_segmento` deve ser filtrada para Alto/Crítico. Sem HHT, volume não equivale a taxa.

Por esse motivo, foi criada a tabela analítica gold.risco_por_segmento, que permite segmentar as ocorrências por nível de risco e direcionar a análise especificamente para os eventos classificados como Alto ou Crítico. Essa abordagem é mais adequada para a gestão de riscos, pois concentra a atenção nos eventos com maior potencial de causar lesões graves, fatalidades ou impactos operacionais significativos.

Sob a ótica da gestão preventiva, a análise por criticidade permite direcionar recursos de forma mais eficiente. Caso determinado segmento apresente concentração elevada de eventos classificados como Alto ou Crítico, os gestores podem priorizar ações como:
Revisão de procedimentos operacionais;
Reforço dos treinamentos obrigatórios;
Atualização das Análises Preliminares de Risco (APR);
Intensificação de inspeções e auditorias de campo;
Ampliação do uso de tecnologias de redução da exposição ao risco;
Revisão dos controles relacionados às Regras de Ouro mais associadas aos eventos. 

> **Screenshot 8:** inserir consulta Gold com riscos Alto e Crítico.

### 6.3 Pergunta 3
A análise da tabela gold.regras_de_ouro demonstrou que os temas mais recorrentes associados às ocorrências de segurança foram 8 - Veículos e Equipamentos Móveis, com 722 registros, e 2 - Percepção de Risco, com 627 registros. Em seguida aparecem 11 - Não se Aplica, 4 - Eletricidade, Outros e 3 - Trabalho em Altura, indicando os principais grupos de fatores relacionados aos eventos registrados ao longo do período analisado.

Sob a perspectiva da gestão de segurança, a frequência elevada desses temas representa um importante direcionador para a tomada de decisão. A premissa utilizada neste MVP é que a recorrência de uma determinada Regra de Ouro não indica necessariamente falha de controle, mas evidencia áreas onde a organização está mais exposta a riscos operacionais ou onde existem maiores oportunidades de fortalecimento das barreiras preventivas.

O destaque de Veículos e Equipamentos Móveis sugere que uma parcela significativa das ocorrências está relacionada a atividades que envolvem deslocamentos, operação de veículos leves e pesados, movimentação de equipamentos e logística operacional. Esse resultado é coerente com a realidade do setor elétrico, onde equipes frequentemente percorrem longas distâncias para inspeções, manutenção de linhas de transmissão, atendimento de emergências e execução de obras.

Do ponto de vista gerencial, esse resultado fornece subsídios para priorizar investimentos e ações preventivas relacionadas à:
Segurança viária;
Gestão de frotas;
Treinamento de direção defensiva;
Operação segura de máquinas e equipamentos;
Monitoramento de comportamento dos condutores;
Avaliação de riscos de deslocamento e mobilização de equipes.

A segunda categoria mais frequente, Percepção de Risco, possui relevância estratégica ainda maior, pois está diretamente relacionada ao comportamento humano e à capacidade dos trabalhadores de identificar perigos antes da execução das atividades. A elevada quantidade de registros associados a esse tema sugere que muitos eventos poderiam estar relacionados a falhas de identificação, avaliação ou comunicação dos riscos presentes no ambiente de trabalho.

Essa informação é particularmente importante para os tomadores de decisão, pois indica que ações focadas apenas em controles físicos ou procedimentais podem não ser suficientes. Nesses casos, torna-se necessário fortalecer elementos da cultura de segurança, tais como:

Desenvolvimento da percepção de risco;
Cumprimento das APRs (Análises Preliminares de Risco);
Prática sistemática do "Pare e Pense";
Qualidade dos diálogos de segurança;
Liderança em campo;
Programas de observação comportamental.

A combinação entre Veículos e Equipamentos Móveis e Percepção de Risco indica que uma parcela significativa do potencial preventivo da organização está associada tanto aos riscos operacionais de mobilidade quanto às decisões tomadas pelos trabalhadores durante a execução das atividades. Sob essa ótica, a priorização de recursos deve buscar um equilíbrio entre melhorias técnicas e fortalecimento da cultura de segurança.

Outro aspecto relevante para a tomada de decisão é que as Regras de Ouro funcionam como um mecanismo de priorização gerencial. Em vez de dispersar esforços em dezenas de temas simultaneamente, a organização pode concentrar parte dos investimentos, treinamentos e campanhas nos assuntos que demonstraram maior recorrência histórica na base de dados. Essa estratégia aumenta a eficiência da gestão e potencializa o retorno das ações preventivas implementadas.

![Ranking das Regras de Ouro](evidencias/ranking_regras_de_ouro.png)

### 6.4 Pergunta 4
A análise dos registros demonstra que 1.254 ocorrências (63,0%) estão associadas a trabalhadores terceiros, enquanto 735 ocorrências (37,0%) estão relacionadas a empregados próprios. Em uma primeira análise, esse resultado evidencia que a maior parte dos eventos registrados na base ocorreu em atividades executadas por empresas contratadas. Entretanto, sob uma perspectiva acadêmica e de gestão de segurança, essa informação deve ser interpretada com cautela para evitar conclusões equivocadas.

Sob a ótica da tomada de decisão, o resultado obtido possui relevância significativa porque evidencia a participação expressiva das empresas contratadas nas operações da organização. No setor elétrico, atividades de construção, manutenção, montagem, inspeção e apoio operacional frequentemente são realizadas por terceiros, o que naturalmente aumenta sua presença nos registros de segurança. Dessa forma, o resultado pode refletir não apenas exposição ao risco, mas também a própria configuração operacional da empresa.

Mesmo sem permitir comparações de taxas, a distribuição observada fornece informações importantes para a gestão. O fato de quase dois terços dos registros estarem associados a terceiros indica a necessidade de atenção especial aos processos de gestão de contratadas, incluindo:
Capacitação e qualificação em segurança;
Integração e ambientação de SST;
Fiscalização do cumprimento de procedimentos;
Avaliação de desempenho dos contratos;
Supervisão de atividades críticas;
Disseminação das Regras de Ouro;
Programas de cultura e liderança em segurança.

Outro aspecto relevante é que a análise pode indicar oportunidades para fortalecer a padronização dos requisitos de segurança entre empregados próprios e contratados. Organizações com sistemas de gestão maduros buscam garantir que todos os trabalhadores, independentemente de vínculo empregatício, estejam submetidos aos mesmos padrões de controle, treinamento e gestão de riscos.

Do ponto de vista preventivo, essa informação permite direcionar recursos para os grupos com maior participação nas atividades operacionais. Caso a maior parte da exposição organizacional esteja concentrada em empresas terceiras, torna-se estratégico ampliar investimentos em treinamento, monitoramento e desenvolvimento da cultura de segurança nesse público, potencializando o retorno das ações preventivas.

Esse resultado reforça um conceito fundamental da análise de indicadores de segurança: valores absolutos não devem ser confundidos com indicadores de desempenho. Uma quantidade maior de registros pode simplesmente refletir maior número de trabalhadores, maior volume de serviços executados ou uma cultura de reporte mais desenvolvida. Sem um denominador de exposição, qualquer comparação direta entre próprios e terceiros estaria sujeita a viés de interpretação.

### 6.5 Discussão dos resultados
O MVP permite priorização e exploração, mas não comparação definitiva de desempenho. Ausências em gravidade e atributos causais limitam análises aprofundadas.

## 7. Autoavaliação
### 7.1 Objetivos atingidos
Ingestão, Delta, arquitetura Medalhão, limpeza, catálogo, qualidade e análises foram estruturados.
### 7.2 Dificuldades
Heterogeneidade das categorias, campos ausentes e anonimização.
### 7.3 Limitações
Ausência de HHT, 2026 parcial, lacunas clínicas/causais, falta de chave técnica e restrição de uso.
### 7.4 Trabalhos futuros
Integrar HHT, calcular TF/TFCA/TG, criar dimensões, automatizar testes e montar dashboard no Databricks SQL.

## Estrutura e ordem de execução
```text
mvp-pipeline-acidentes/
├── README.md
├── notebooks/00_setup.sql
├── notebooks/01_bronze.py
├── notebooks/02_silver.py
├── notebooks/03_gold_analise.sql
├── notebooks/04_qualidade.sql
├── evidencias/eventos_por_ano.png
├── evidencias/ranking_regras_de_ouro.png
├── data_private/LEIA-ME.md
└── .gitignore
```



