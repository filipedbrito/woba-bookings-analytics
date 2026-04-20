{% macro incremental_filter(column) %}

    {% if is_incremental() %}
        where {{ column }} > (select max({{ column }}) from {{ this }})
    {% endif %}

{% endmacro %}