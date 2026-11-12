select
    id as parcela_id,
    contrato_id,
    numero,
    valor,
    vencimento,
    data_pagamento,
    status,
    created_at,
    updated_at
from {{ source('silver', 'parcelas') }}
where not is_deleted
