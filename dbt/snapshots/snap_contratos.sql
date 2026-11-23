{% snapshot snap_contratos %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

-- Fonte: Silver (estado atual, 1 linha por id).
-- O snapshot compara a Silver de hoje com a última foto e guarda cada mudança como uma nova versão.
select * from {{ source('silver', 'contratos') }}

{% endsnapshot %}
