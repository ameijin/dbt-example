{% macro calculate_mrr(amount_column, billing_period_column) %}
    case
        when {{ billing_period_column }} = 'annual'
            then round({{ amount_column }} / 12, 2)
        else {{ amount_column }}
    end
{% endmacro %}
