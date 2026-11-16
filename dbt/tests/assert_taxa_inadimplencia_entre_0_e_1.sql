-- Teste singular: devolve as linhas que VIOLAM a regra. Se devolver alguma, o teste falha.
select *
from {{ ref('mart_inadimplencia_mensal') }}
where taxa_inadimplencia < 0 or taxa_inadimplencia > 1
