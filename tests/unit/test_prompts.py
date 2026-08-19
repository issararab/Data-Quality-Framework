from lakescore.generators.prompts import (
    COLUMN_CHECK_RAG_SYSTEM_PROMPT,
    COLUMN_DESCRIPTION_SYSTEM_PROMPT,
)


def test_prompts_declare_a_context_placeholder():
    # Both prompts are formatted with {"context": ...} by their respective chains.
    assert "{context}" in COLUMN_DESCRIPTION_SYSTEM_PROMPT
    assert "{context}" in COLUMN_CHECK_RAG_SYSTEM_PROMPT


def test_check_rag_prompt_requires_output_to_start_with_dash():
    # This is the contract generate_soda_checks_yaml relies on when assembling YAML.
    assert '"  -"' in COLUMN_CHECK_RAG_SYSTEM_PROMPT
