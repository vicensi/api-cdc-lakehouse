-- Fato de parcelas. Grão: 1 linha por parcela, com contrato e cliente já anexados.
with parcelas as (
    select * from {{ ref('stg_parcelas') }}
),

contratos as (
    select * from {{ ref('stg_contratos') }}
),

clientes as (
    select * from {{ ref('stg_clientes') }}
)

select
    p.parcela_id,
    p.contrato_id,
    c.cliente_id,
    cl.cidade,
    p.numero,
    p.valor,
    p.vencimento,
    cast(date_trunc('month', p.vencimento) as date)  as mes_vencimento,
    p.data_pagamento,
    p.status,
    c.status                                         as contrato_status,
    case
        when p.status = 'paga'     then greatest(datediff(p.data_pagamento, p.vencimento), 0)
        when p.status = 'atrasada' then greatest(datediff(current_date(), p.vencimento), 0)
        else 0
    end                                              as dias_atraso
from parcelas p
join contratos c  on p.contrato_id = c.contrato_id
join clientes cl  on c.cliente_id = cl.cliente_id
