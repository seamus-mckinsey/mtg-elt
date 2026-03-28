select
    _dlt_parent_id,
    name          as card_name,
    sanitized     as card_slug,
    num_decks,
    potential_decks,
    synergy       as synergy_score,
    cmc,
    type_line,
    'high_synergy' as recommendation_type
from {{ source('edhrec', 'commanders__high_synergy_cards__high_synergy_cards') }}
