-- Schema exploration for dlt-normalized EDHRec tables.
--
-- Run this directly in DuckDB after loading data to verify the actual
-- child table names before running dbt models.
--
-- Usage:
--   duckdb db/edhrec.db < transform/analyses/explore_edhrec_schema.sql
-- Or interactively:
--   duckdb db/edhrec.db
--   .read transform/analyses/explore_edhrec_schema.sql

-- 1. All tables in the edhrec schema
select table_name
from information_schema.tables
where table_schema = 'edhrec'
order by table_name;

-- 2. Columns on the commanders table
-- pragma table_info('edhrec.commanders');

-- 3. Sample a card recommendation child table to verify column names
-- select * from edhrec.commanders__high_synergy_cards__high_synergy_cards limit 5;

-- 4. Row counts per child table (run individually)
-- select 'high_synergy'     as tbl, count(*) from edhrec.commanders__high_synergy_cards__high_synergy_cards
-- union all
-- select 'top_cards'        as tbl, count(*) from edhrec.commanders__top_cards__top_cards
-- union all
-- select 'creatures'        as tbl, count(*) from edhrec.commanders__top_creatures__creatures;
