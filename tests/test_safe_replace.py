from fabric_agent.refactor.safe_rename_engine import _safe_bracket_replace

def test_safe_bracket_replace_exact_only():
    text = "SUMX(Sales, [Sales] + [SalesTax])"
    new_text, n = _safe_bracket_replace(text, "[Sales]", "[Gross Sales]")
    assert n == 1
    assert "[Gross Sales]" in new_text
    assert "[SalesTax]" in new_text
