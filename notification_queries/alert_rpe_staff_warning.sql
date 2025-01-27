with parsed_result as (
select distinct
        rl.created_date,
        pd.protocol_id as protocol_id,
        rl.status,
        c.email_address as rpe_submitter_email,
        substr(substr(rl.result, instr(rl.result, ' --- ') + 1), 4) as parsed_result
from oncore.onc_rpe_log rl
join oncore.rv_contact c
    on lower(rl.created_user) = lower(c.institution_staff_id)
join oncore.onc_rpe_pcl_dest pd
    on pd.last_success_log_id = rl.id
where entity = 'PROTOCOL_STATUS' and result like '%Warnings%'
and rl.created_date >= sysdate - INTERVAL '30' minute(1)
order by rl.created_date desc),

sep_rows as (
select distinct
  pr.protocol_id,
  pr.status,
  pr.created_date,
  pr.rpe_submitter_email,
  trim(regexp_substr(pr.parsed_result, '[^---]+', 1, levels.column_value)) as warning
from
  parsed_result pr,
  table(cast(multiset(select level from dual connect by  level <= regexp_count(pr.parsed_result, '[^---]+')  + 1) as sys.OdciNumberList)) levels
order by protocol_id),

final as (
    select distinct
        protocol_id,
        created_date,
        rpe_submitter_email,
        case
            when warning like '%staff%' then trim(substr(warning, instr(warning, ': ') + 1))
            else 'not applicable'
        end as staff_role,
        case
            when warning like '%staff%' then protocol_id || '_' || trim(substr(warning, instr(warning, ': ') + 1))
            else 'not applicable'
        end rpe_message_role_key
    from sep_rows
    where warning is not null
    order by protocol_id
),

missing_staff_ids as (
select *
    from oncore_report_ro.fct_active_protocol_staff_assignments
    where contact_identifer_type <> 'not applicable' and contact_identifier_value = 'not reported'
)

select distinct
    sa.protocol_no,
    f.created_date as rpe_sent_date,
    f."RPE_SUBMITTER_EMAIL",
    f.staff_role,
    sa.staff_full_name,
    sa.oncore_contact_detail_url
from final f
join missing_staff_ids sa
    on sa.rpe_message_role_key = f.rpe_message_role_key
order by f.created_date desc