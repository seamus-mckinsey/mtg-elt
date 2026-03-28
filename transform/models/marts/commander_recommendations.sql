-- Commanders ranked by EDHRec popularity.
-- Lower edhrec_rank = more widely played on EDHRec.
--
-- Use this to answer: "which commander should I build next?"
-- Filter by mana_cost symbols to find commanders in your preferred colors,
-- or filter by type_line to narrow by creature type or mechanic.

select
    c.oracle_id,
    c.name                                        as commander_name,
    c.mana_cost,
    c.cmc,
    c.type_line,
    c.oracle_text,
    c.rarity,
    c.edhrec_rank,
    c.set_name,
    count(distinct r.card_name)                   as num_recommended_cards,
    count(distinct case
        when r.recommendation_type = 'high_synergy' then r.card_name
    end)                                          as num_high_synergy_cards,
    round(avg(case
        when r.synergy_score is not null then r.synergy_score
    end), 4)                                      as avg_synergy_score
from {{ ref('stg_commanders') }} c
left join {{ ref('int_commander_card_recommendations') }} r
    on r.commander_oracle_id = c.oracle_id
group by all
order by c.edhrec_rank asc nulls last
