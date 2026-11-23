-- Falha (retorna linhas) se o historico de clientes estiver inconsistente.

-- 1) mais de uma versao aberta para o mesmo cliente
select id, 'versao aberta duplicada' as problema
from {{ ref('snap_clientes') }}
where dbt_valid_to is null
group by id
having count(*) > 1

union all

-- 2) janelas de validade que se sobrepoem para o mesmo cliente
select a.id, 'janelas sobrepostas' as problema
from {{ ref('snap_clientes') }} a
join {{ ref('snap_clientes') }} b
  on a.id = b.id
 and a.dbt_scd_id < b.dbt_scd_id
 and a.dbt_valid_from < coalesce(b.dbt_valid_to, timestamp'9999-12-31')
 and b.dbt_valid_from < coalesce(a.dbt_valid_to, timestamp'9999-12-31')

union all

-- 3) cliente da Silver sem versao aberta no snapshot (snapshot atrasado ou perdeu o cliente)
select s.id, 'cliente da silver sem versao vigente' as problema
from {{ source('silver', 'clientes') }} s
left join {{ ref('snap_clientes') }} n
  on s.id = n.id and n.dbt_valid_to is null
where n.id is null
