-- Staging: renomeia a chave e descarta registros apagados (soft delete) na origem.
select
    id as cliente_id,
    nome,
    renda_mensal,
    cidade,
    score,
    created_at,
    updated_at
from {{ source('silver', 'clientes') }}
where not is_deleted
