select
    id as contrato_id,
    cliente_id,
    valor_principal,
    taxa_mensal,
    n_parcelas,
    status,
    created_at,
    updated_at
from {{ source('silver', 'contratos') }}
where not is_deleted
