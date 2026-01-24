{% macro cents_to_dollars(column_name, decimal_places=2) %}
    {#
     Convert cents to dollars

     Args:
       column_name: Column containing cents
       decimal_places: Decimal precision (default: 2)

     Returns:
       round(column_name / 100.0, decimal_places)

     Example:
       select cents_to_dollars('price_cents') as price_dollars
     #}
     round({{ column_name }} / 100.0, {{ decimal_places }})
{% endmacro %}