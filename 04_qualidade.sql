SELECT COUNT(*) total, SUM(data_evento IS NULL) data_nula, SUM(gravidade IS NULL) gravidade_nula, SUM(agente_causador IS NULL) agente_nulo, SUM(tipo_de_lesao IS NULL) lesao_nula, SUM(parte_do_corpo_atingida IS NULL) parte_nula FROM mvp_seguranca.silver.acidentes_limpos;
SELECT 'idade_fora_dominio' teste,COUNT(*) falhas FROM mvp_seguranca.silver.acidentes_limpos WHERE idade IS NOT NULL AND NOT(idade BETWEEN 14 AND 90)
UNION ALL SELECT 'vinculo_fora_dominio',COUNT(*) FROM mvp_seguranca.silver.acidentes_limpos WHERE empregado IS NOT NULL AND empregado NOT IN ('Próprio','Terceiro')
UNION ALL SELECT 'risco_fora_dominio',COUNT(*) FROM mvp_seguranca.silver.acidentes_limpos WHERE grau_de_risco IS NOT NULL AND grau_de_risco NOT IN ('Baixo','Médio','Alto','Crítico');
