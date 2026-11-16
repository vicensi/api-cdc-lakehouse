-- Indicador de negócio. Grão: 1 linha por mês de vencimento.
-- Regra: inadimplente = parcela com status 'atrasada'; só entram parcelas já vencidas.
select
    mes_vencimento,
    count(*)                                                           as qtd_parcelas,
    sum(valor)                                                         as valor_total,
    sum(case when status = 'atrasada' then 1 else 0 end)               as qtd_atrasadas,
    sum(case when status = 'atrasada' then valor else 0 end)           as valor_atrasado,
    round(
        sum(case when status = 'atrasada' then valor else 0 end) / sum(valor), 4
    )                                                                  as taxa_inadimplencia
from {{ ref('fct_parcelas') }}
where vencimento <= current_date()
group by mes_vencimento
