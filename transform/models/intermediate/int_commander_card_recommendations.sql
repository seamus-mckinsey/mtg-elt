-- Flattens all dlt-normalized EDHRec card recommendation tables into one view.
--
-- dlt normalizes the nested card list data as follows:
--   field stored in edhrec.py: card_data["high_synergy_cards"] = {"High Synergy Cards": [...]}
--   dlt flattens the outer dict and creates a child table for the list:
--     table: commanders__high_synergy_cards__high_synergy_cards
--     FK:    _dlt_parent_id → commanders._dlt_id
--
-- The same pattern applies to all 12 card list fields.
-- If table names differ from expected, run analyses/explore_edhrec_schema.sql to find them.
--
-- Each card row has: name, sanitized (url slug), num_decks, potential_decks,
--   synergy (EDHRec synergy score), cmc, type_line

with commanders as (
    select oracle_id, name as commander_name, _dlt_id
    from {{ source('edhrec', 'commanders') }}
),

high_synergy as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'high_synergy' as recommendation_type
    from {{ source('edhrec', 'commanders__high_synergy_cards__high_synergy_cards') }}
),

top_cards as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'top_cards' as recommendation_type
    from {{ source('edhrec', 'commanders__top_cards__top_cards') }}
),

top_creatures as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'creature' as recommendation_type
    from {{ source('edhrec', 'commanders__top_creatures__creatures') }}
),

top_instants as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'instant' as recommendation_type
    from {{ source('edhrec', 'commanders__top_instants__instants') }}
),

top_sorceries as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'sorcery' as recommendation_type
    from {{ source('edhrec', 'commanders__top_sorceries__sorceries') }}
),

top_enchantments as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'enchantment' as recommendation_type
    from {{ source('edhrec', 'commanders__top_enchantments__enchantments') }}
),

top_artifacts as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'utility_artifact' as recommendation_type
    from {{ source('edhrec', 'commanders__top_artifacts__utility_artifacts') }}
),

top_mana_artifacts as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'mana_artifact' as recommendation_type
    from {{ source('edhrec', 'commanders__top_mana_artifacts__mana_artifacts') }}
),

top_planeswalkers as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'planeswalker' as recommendation_type
    from {{ source('edhrec', 'commanders__top_planeswalkers__planeswalkers') }}
),

top_utility_lands as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'utility_land' as recommendation_type
    from {{ source('edhrec', 'commanders__top_utility_lands__utility_lands') }}
),

top_lands as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'land' as recommendation_type
    from {{ source('edhrec', 'commanders__top_lands__lands') }}
),

new_cards as (
    select _dlt_parent_id, name, sanitized, num_decks, potential_decks, synergy, cmc, type_line,
           'new_card' as recommendation_type
    from {{ source('edhrec', 'commanders__new_cards__new_cards') }}
),

all_recs as (
    select * from high_synergy
    union all select * from top_cards
    union all select * from top_creatures
    union all select * from top_instants
    union all select * from top_sorceries
    union all select * from top_enchantments
    union all select * from top_artifacts
    union all select * from top_mana_artifacts
    union all select * from top_planeswalkers
    union all select * from top_utility_lands
    union all select * from top_lands
    union all select * from new_cards
)

select
    c.oracle_id              as commander_oracle_id,
    c.commander_name,
    r.name                   as card_name,
    r.sanitized              as card_slug,
    r.num_decks,
    r.potential_decks,
    case
        when r.potential_decks > 0
        then round(r.num_decks::double / r.potential_decks, 4)
    end                      as inclusion_rate,
    r.synergy                as synergy_score,
    r.cmc,
    r.type_line,
    r.recommendation_type
from commanders c
join all_recs r on r._dlt_parent_id = c._dlt_id
