-- Card recommendations per commander, deduplicated and scored.
--
-- Use this to answer:
--   "what cards should I add to my [commander] deck?"
--   "what new cards work well with [commander]?"
--
-- Key columns:
--   synergy_score    — EDHRec synergy: how much MORE often this card appears
--                      in decks with this commander vs. all Commander decks.
--                      Positive = commander-specific staple; negative = avoid.
--   inclusion_rate   — fraction of eligible decks that include this card (0–1).
--   recommendation_type — source list: high_synergy, top_cards, creature,
--                         instant, sorcery, enchantment, utility_artifact,
--                         mana_artifact, planeswalker, utility_land, land, new_card.
--
-- A card can appear in multiple recommendation lists (e.g. both high_synergy
-- and top_cards). We keep the row with the highest synergy_score.

with ranked as (
    select
        r.commander_oracle_id,
        r.commander_name,
        r.card_name,
        r.card_slug,
        oc.mana_cost,
        oc.oracle_text,
        oc.rarity,
        r.cmc,
        r.type_line,
        r.inclusion_rate,
        r.synergy_score,
        r.num_decks,
        r.potential_decks,
        r.recommendation_type,
        row_number() over (
            partition by r.commander_oracle_id, r.card_name
            order by r.synergy_score desc nulls last
        ) as rn
    from {{ ref('int_commander_card_recommendations') }} r
    left join {{ ref('stg_oracle_cards') }} oc on oc.name = r.card_name
)

select
    commander_oracle_id,
    commander_name,
    card_name,
    card_slug,
    mana_cost,
    oracle_text,
    rarity,
    cmc,
    type_line,
    inclusion_rate,
    synergy_score,
    recommendation_type,
    num_decks,
    potential_decks
from ranked
where rn = 1
order by commander_oracle_id, synergy_score desc nulls last
