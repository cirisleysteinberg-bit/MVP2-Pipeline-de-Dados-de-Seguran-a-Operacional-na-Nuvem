CREATE OR REPLACE TABLE mvp_seguranca.gold.eventos_por_ano AS SELECT ano,tipo_evento,COUNT(*) quantidade FROM mvp_seguranca.silver.acidentes_limpos GROUP BY ano,tipo_evento;
CREATE OR REPLACE TABLE mvp_seguranca.gold.risco_por_segmento AS SELECT COALESCE(segmento,'Não informado') segmento,COALESCE(grau_de_risco,'Não informado') grau_de_risco,COUNT(*) quantidade FROM mvp_seguranca.silver.acidentes_limpos GROUP BY ALL;
CREATE OR REPLACE TABLE mvp_seguranca.gold.regras_de_ouro AS SELECT COALESCE(compromissos_regra_de_ouro,'Não informado') regra_de_ouro,COUNT(*) quantidade FROM mvp_seguranca.silver.acidentes_limpos GROUP BY ALL;
CREATE OR REPLACE TABLE mvp_seguranca.gold.proprios_terceiros AS SELECT COALESCE(empregado,'Não informado') vinculo,tipo_evento,COUNT(*) quantidade FROM mvp_seguranca.silver.acidentes_limpos GROUP BY ALL;
SELECT * FROM mvp_seguranca.gold.eventos_por_ano ORDER BY ano;
SELECT * FROM mvp_seguranca.gold.risco_por_segmento WHERE grau_de_risco IN ('Alto','Crítico') ORDER BY quantidade DESC;
SELECT * FROM mvp_seguranca.gold.regras_de_ouro ORDER BY quantidade DESC LIMIT 15;
