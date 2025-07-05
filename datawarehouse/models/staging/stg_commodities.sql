-- import

with source as (
    select
        "Date",
        "Close",
        "symbol"
    from 
        {{ source ('sales_8mwg', 'commodities') }}
),

renamed as (

    select
        cast("Date" as date) as data,
        "Close" as valor_fechamento,
        symbol
    from
        source
)

select * from renamed