-- models/staging/stg_movimentacao_commodities.sql

with source as (
    select
        date,
        symbol,
        action,
        quantity
    from 
        {{ source('sales_8mwg', 'commodities_transactions') }}
),

renamed as (
    select
        cast(date as date) as data,
        symbol,
        action as acao,
        quantity as quantidade
    from source
)

select * from renamed