select
    oracle_id,
    name,
    mana_cost,
    cmc,
    type_line,
    oracle_text,
    power,
    toughness,
    rarity,
    "set"          as set_code,
    set_name,
    set_type,
    released_at,
    artist,
    edhrec_rank,
    reserved,
    game_changer
from {{ source('scryfall', 'oracle_cards') }}
