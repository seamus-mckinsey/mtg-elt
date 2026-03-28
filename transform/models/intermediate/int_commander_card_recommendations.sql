-- Joins commanders with all EDHRec card recommendation staging models.
-- Uses dbt_utils.union_relations to combine the 12 card-type staging models;
-- source_column_name is suppressed since recommendation_type carries that context.

with commanders as (
    select oracle_id, name as commander_name, _dlt_id
    from {{ source('edhrec', 'commanders') }}
),

all_recs as (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_commander_high_synergy_cards'),
            ref('stg_commander_top_cards'),
            ref('stg_commander_top_creatures'),
            ref('stg_commander_top_instants'),
            ref('stg_commander_top_sorceries'),
            ref('stg_commander_top_enchantments'),
            ref('stg_commander_top_artifacts'),
            ref('stg_commander_top_mana_artifacts'),
            ref('stg_commander_top_planeswalkers'),
            ref('stg_commander_top_utility_lands'),
            ref('stg_commander_top_lands'),
            ref('stg_commander_new_cards'),
        ],
        source_column_name=none
    ) }}
)

select
    c.oracle_id              as commander_oracle_id,
    c.commander_name,
    r.card_name,
    r.card_slug,
    r.num_decks,
    r.potential_decks,
    case
        when r.potential_decks > 0
        then round(r.num_decks::double / r.potential_decks, 4)
    end                      as inclusion_rate,
    r.synergy_score,
    r.cmc,
    r.type_line,
    r.recommendation_type
from commanders c
join all_recs r on r._dlt_parent_id = c._dlt_id
