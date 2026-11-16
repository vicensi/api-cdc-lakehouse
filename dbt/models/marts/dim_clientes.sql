-- Dimensão de clientes. Grão: 1 linha por cliente.
with clientes as (
    select * from {{ ref('stg_clientes') }}
),

contratos as (
    select
        cliente_id,
        count(*)                                                  as qtd_contratos,
        sum(valor_principal)                                      as valor_total_contratado,
        sum(case when status = 'inadimplente' then 1 else 0 end)  as qtd_contratos_inadimplentes
    from {{ ref('stg_contratos') }}
    group by cliente_id
)

select
    c.cliente_id,
    c.nome,
    c.cidade,
    c.renda_mensal,
    c.score,
    case
        when c.score >= 700 then 'baixo'
        when c.score >= 500 then 'medio'
        else 'alto'
    end                                              as faixa_risco,
    coalesce(k.qtd_contratos, 0)                     as qtd_contratos,
    coalesce(k.valor_total_contratado, 0)            as valor_total_contratado,
    coalesce(k.qtd_contratos_inadimplentes, 0)       as qtd_contratos_inadimplentes
from clientes c
left join contratos k on c.cliente_id = k.cliente_id
