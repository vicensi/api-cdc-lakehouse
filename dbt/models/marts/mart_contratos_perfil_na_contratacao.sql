-- Para cada contrato: o perfil do cliente NO MOMENTO da contratacao (join "ponto no tempo").
-- Grao: 1 linha por contrato.
-- LEFT JOIN de proposito: contratos anteriores ao inicio do snapshot ficam sem perfil (perfil_encontrado = false).
select
    c.contrato_id,
    c.cliente_id,
    c.valor_principal,
    c.status,
    c.created_at                  as contratado_em,
    h.score                       as score_na_contratacao,
    h.renda_mensal                as renda_na_contratacao,
    h.faixa_risco                 as faixa_risco_na_contratacao,
    h.cliente_versao_id is not null as perfil_encontrado
from {{ ref('stg_contratos') }} c
left join {{ ref('dim_clientes_historico') }} h
  on  h.cliente_id = c.cliente_id
  and c.created_at >= h.vigente_desde
  and c.created_at <  coalesce(h.vigente_ate, timestamp'9999-12-31')
