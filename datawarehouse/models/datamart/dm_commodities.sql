-- models/datamart/dm_commodities.sql

with commodities as (
    select
        data,
        symbol,
        valor_fechamento
    from 
        {{ ref('stg_commodities') }}
),

movimentacao as (
    select
        data,
        symbol,
        acao,
        quantidade
    from 
        {{ ref('stg_movimentacoes_commodities') }}
),

joined as (
    select
        c.data,
        c.symbol,
        c.valor_fechamento,
        m.acao,
        m.quantidade,
        (m.quantidade * c.valor_fechamento) as valor,
        case
            when m.acao = 'sell' then (m.quantidade * c.valor_fechamento)
            else -(m.quantidade * c.valor_fechamento)
        end as ganho
    from
        commodities c
    inner join
        movimentacao m
    on
        c.data = m.data
        and c.symbol = m.symbol
),

last_day as (
    select
        max(data) as max_date
    from
        joined
),

filtered as (
    select
        *
    from
        joined
    where
        data = (select max_date from last_day)
)

select
    data,
    symbol,
    valor_fechamento,
    acao,
    quantidade,
    valor,
    ganho
from
    filtered
