-- Commander-eligible cards enriched with EDHRec data.
-- Joins the EDHRec commanders table back to oracle_cards to get full card attributes,
-- since edhrec.py reads from scryfall.db and the two DBs are in separate files.
select
    c.oracle_id,
    c.name,
    c._dlt_id,
    oc.mana_cost,
    oc.cmc,
    oc.type_line,
    oc.oracle_text,
    oc.power,
    oc.toughness,
    oc.rarity,
    oc.set_code,
    oc.set_name,
    oc.edhrec_rank
from {{ source('edhrec', 'commanders') }} c
left join {{ ref('stg_oracle_cards') }} oc using (oracle_id)
