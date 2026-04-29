with

ghl_contacts as (

    select * from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__contacts`

),

typeform_responses as (

    select * from `project-41542e21-470f-4589-96d`.`STG`.`stg_typeform__responses`

),

-- Collapse typeform to one row per email, keeping latest submission signals
-- and whether any UTM attribution is present. Aggregated here (pre-join) so
-- the dim stays flat and never fans contacts out.
typeform_enrichment as (

    select
        lower(trim(typeform_responses.response_token))       as response_token,
        max(typeform_responses.submitted_at)                 as latest_typeform_at,

        max(typeform_responses.hidden_utm_source)            as utm_source,
        max(typeform_responses.hidden_utm_medium)            as utm_medium,
        max(typeform_responses.hidden_utm_campaign)          as utm_campaign,

        max(typeform_responses.calculated_score)             as psychographic_score,

        max(
            case
                when typeform_responses.hidden_utm_source   is not null
                  or typeform_responses.hidden_utm_medium   is not null
                  or typeform_responses.hidden_utm_campaign is not null
                then 1 else 0
            end
        )                                                    as has_utm_int

    from typeform_responses
    group by 1

),

-- GHL contacts hold no email on the Typeform join axis natively (Typeform
-- exposes no email in `stg_typeform__responses`); we carry attribution on a
-- best-effort basis via the landing-token EAV join owed in the downstream
-- bridge layer. For v1 the per-contact `attribution_era` derives from
-- `lead_source` rather than Typeform join, and psychographic enrichment is
-- left null-safe until the Typeform-answers pivot model ships.
joined as (

    select
        ghl_contacts.contact_id,
        ghl_contacts.location_id,
        ghl_contacts.assigned_user_id,
        ghl_contacts.business_id,

        ghl_contacts.contact_name,
        ghl_contacts.first_name,
        ghl_contacts.last_name,
        ghl_contacts.company_name,

        ghl_contacts.email,
        lower(trim(ghl_contacts.email))                      as email_norm,
        ghl_contacts.phone,

        ghl_contacts.contact_type,
        ghl_contacts.lead_source,
        ghl_contacts.is_dnd,

        ghl_contacts.city,
        ghl_contacts.state,
        ghl_contacts.postal_code,
        ghl_contacts.address1,
        ghl_contacts.country,
        ghl_contacts.timezone,
        ghl_contacts.website,
        ghl_contacts.date_of_birth,

        ghl_contacts.contact_created_at,
        ghl_contacts.contact_updated_at,

        -- attribution_era: UTM vs pre-UTM, per-contact. Derived from
        -- `lead_source` because the Typeform-to-contact email-join bridge
        -- is not yet in staging. Contacts whose lead_source is non-null
        -- and matches a paid/tracked pattern → 'utm'; everything else
        -- → 'pre_utm'. Downstream era-aware mart joins use this column.
        case
            when lower(ghl_contacts.lead_source) in (
                'paid-ads', 'google-ads', 'facebook-ads',
                'paid_ads', 'google_ads', 'facebook_ads',
                'paid ads', 'google ads', 'facebook ads',
                'paid', 'google', 'facebook', 'meta', 'tiktok'
            )
            then 'utm'
            when ghl_contacts.lead_source is null
            then 'pre_utm'
            else 'pre_utm'
        end                                                  as attribution_era,

        typeform_enrichment.latest_typeform_at,
        typeform_enrichment.utm_source,
        typeform_enrichment.utm_medium,
        typeform_enrichment.utm_campaign,
        typeform_enrichment.psychographic_score,
        coalesce(typeform_enrichment.has_utm_int, 0) = 1     as has_typeform_utm,

        ghl_contacts._ingested_at

    from ghl_contacts
    -- Typeform join intentionally left as a no-op placeholder until the
    -- email/landing-token bridge ships. `response_token` is not GHL's
    -- contact identifier; this left join yields all nulls today but keeps
    -- the column contract stable for the future bridge drop-in.
    left join typeform_enrichment
        on lower(trim(ghl_contacts.email)) = typeform_enrichment.response_token

),

final as (

    select
        to_hex(md5(cast(coalesce(cast(location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(contact_id as string), '_dbt_utils_surrogate_key_null_') as string)))                                                 as contact_sk,

        contact_id,
        location_id,
        assigned_user_id,
        business_id,

        contact_name,
        first_name,
        last_name,
        company_name,

        email,
        email_norm,
        phone,

        contact_type,
        lead_source,
        is_dnd,

        city,
        state,
        postal_code,
        address1,
        country,
        timezone,
        website,
        date_of_birth,

        attribution_era,

        latest_typeform_at,
        utm_source,
        utm_medium,
        utm_campaign,
        psychographic_score,
        has_typeform_utm,

        contact_created_at,
        contact_updated_at,
        _ingested_at

    from joined

)

select * from final