-- Dimensao SCD2 de clientes. Grao: 1 linha por VERSAO do cliente (cliente_id + janela de validade).
select
    cliente_versao_id,
    cliente_id,
    renda_mensal,
    cidade,
    score,
    case
        when score >= 700 then 'baixo'
        when score >= 500 then 'medio'
        else 'alto'
    end                           as faixa_risco,
    is_deleted,
    vigente_desde,
    vigente_ate,
    is_atual
from {{ ref('stg_clientes_historico') }}
