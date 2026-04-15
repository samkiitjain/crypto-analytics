{% macro generate_schema_name(custom_schema_name, node) -%}

    {#
        This macro overrides dbt's default schema naming behaviour.

        dbt default behaviour would produce:
            {profile_dataset}_{custom_schema_name}
            e.g. crypto_analytics_2026_dev_crypto_marts_crypto_staging  ← wrong

        We want it to produce the exact Terraform-generated dataset names:
            crypto_analytics_2026_{env}_crypto_staging   (staging layer)
            crypto_analytics_2026_{env}_crypto_marts     (intermediate + marts layer)

        The env token comes from target.name — "dev" or "prod".
        This mirrors your Terraform variable "environment".

        Terraform dataset pattern:
            replace("${local.name_prefix}_crypto_staging", "-", "_")
            → crypto_analytics_2026_dev_crypto_staging

        How this macro is invoked:
            dbt reads +schema from dbt_project.yml per folder:
                staging      → custom_schema_name = "crypto_staging"
                intermediate → custom_schema_name = "crypto_marts"
                marts        → custom_schema_name = "crypto_marts"

            We prepend the project prefix + env to produce the full dataset name.
    #}

    {%- set env = target.name -%}
    {%- set project_prefix = "crypto_analytics" -%}

    {%- if custom_schema_name is none -%}
        {# Fallback — should not happen if dbt_project.yml is correct #}
        {{ exceptions.raise_compiler_error(
            "generate_schema_name: custom_schema_name is None for node " ~ node.unique_id ~
            ". Make sure every model folder has +schema set in dbt_project.yml."
        ) }}
    {%- else -%}
        {{ project_prefix }}_{{ env }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}