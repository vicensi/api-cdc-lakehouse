{% snapshot snap_clientes %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

-- Historico do perfil de credito do cliente (renda, cidade, score).
-- "nome" fica de fora de proposito: nao e o que se quer rastrear e e dado pessoal;
-- quem precisar do nome faz join com dim_clientes pelo id.
select
    id,
    renda_mensal,
    cidade,
    score,
    is_deleted,
    created_at,
    updated_at
from {{ source('silver', 'clientes') }}

{% endsnapshot %}
