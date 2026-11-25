-- Staging do snapshot: renomeia as colunas tecnicas do dbt para nomes de negocio.
-- Mantem TODAS as versoes (inclusive as de registros apagados, marcadas em is_deleted).
select
    dbt_scd_id                    as cliente_versao_id,
    id                            as cliente_id,
    renda_mensal,
    cidade,
    score,
    is_deleted,
    dbt_valid_from                as vigente_desde,
    dbt_valid_to                  as vigente_ate,      -- nulo = versao atual
    dbt_valid_to is null          as is_atual
from {{ ref('snap_clientes') }}
