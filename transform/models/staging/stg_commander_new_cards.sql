select
    _dlt_parent_id,
    name          as card_name,
    sanitized     as card_slug,
    num_decks,
    potential_decks,
    synergy       as synergy_score,
    cmc,
    type_line,
    'new_card' as recommendation_type
from {{ source('edhrec', 'commanders__new_cards__new_cards') }}
