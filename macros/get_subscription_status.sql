{% macro get_subscription_status(status_column, canceled_at_column, ended_at_column, trial_end_date_column) %}
    case
        when {{ ended_at_column }} is not null
            then 'churned'
        when {{ canceled_at_column }} is not null
            then 'canceled'
        when {{ status_column }} = 'trial'
            and {{ trial_end_date_column }} >= current_date
            then 'trial'
        when {{ status_column }} = 'trial'
            and {{ trial_end_date_column }} < current_date
            then 'expired_trial'
        when {{ status_column }} = 'past_due'
            then 'past_due'
        when {{ status_column }} = 'active'
            then 'active'
        else {{ status_column }}
    end
{% endmacro %}
