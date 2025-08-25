{{
    config(
      unique_key='postcode',
      strategy='check',
      check_cols='all',
    )
}}

select * from {{ ref('ons') }}

{% endsnapshot %}