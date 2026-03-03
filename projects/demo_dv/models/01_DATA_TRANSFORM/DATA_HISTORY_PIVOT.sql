{{ config(
      alias='HIST_DATA_25_ITEMS_PIVOT'
    , materialized='table')
}}

{% set n_attrs = 26 %}
{% set attrs_list = range(1, n_attrs) %}

{% for m in attrs_list %}     

SELECT
	hist_data.primarykey, 
    hist_data.num_leg,
    hist_data.num_pax,
   	{{ m }} as n_history,
    split_part(hist_data.history_{{m}}::text, '\002'::text, 1) AS history_history_type, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 2) AS history_terminal, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 3) AS history_agent, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 4) AS history_date, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 5) AS history_hour, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 6) AS history_city_office, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 7) AS history_office_number, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 8) AS history_agent_client, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 9) AS history_passenger, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 10) AS history_accepted_without_seat_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 11) AS history_seat_assigned_in_this_update, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 12) AS history_fast_track_type, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 13) AS history_boarding_number, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 14) AS history_standby_number, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 15) AS history_standby_reason, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 16) AS history_air_zone_passenger, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 17) AS history_upgrade, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 18) AS history_cash_payments, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 19) AS history_noboardingpass, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 20) AS history_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 21) AS history_updatedelete_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 22) AS history_free_text, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 23) AS history_cporbn_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 24) AS history_issued_void, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 25) AS history_emd_number, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 26) AS history_upgradingtype, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 27) AS history_upgradeinitialclosing, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 28) AS history_compensation_type, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 29) AS history_bonus_type, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 30) AS history_bonus_type_action, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 31) AS history_sfpd_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 32) AS history_additional_service_paid_type, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 33) AS history_aqq_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 34) AS history_esta_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 35) AS history_esta_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 36) AS history_unuso_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 37) AS history_aqq_infant_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 38) AS history_aqq_infant_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 39) AS history_esta_infant_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 40) AS history_esta_infant_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 41) AS history_apps_country, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 42) AS history_app_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 43) AS history_message_code, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 44) AS history_app_infant_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 45) AS history_app_infant_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 46) AS history_message_code_infant, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 47) AS history_irpa_status, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 48) AS history_irpa_indicator_canceled, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 49) AS history_unsolicited_irpa_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 50) AS history_infant_irpa_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 51) AS history_irpa_status_infant, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 52) AS history_ind_canceled_child, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 53) AS history_unsolicited_irpa_infant_indicator, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 54) AS history_sfpf_status_2, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 55) AS history_edifact_status_message, 
    split_part(hist_data.history_{{m}}::text, '\002'::text, 56) AS history_error_code,
    '{{ ref('history_data') }}' as record_source
from {{ ref('history_data') }} hist_data
where history_history_type != ''

{% if not loop.last %} 
	union all
{% endif %}
{% endfor %}

