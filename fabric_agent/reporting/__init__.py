"""Reporting utilities (HTML artifacts for human-in-the-loop review)."""

from .html import render_safe_refactor_html, render_schema_drift_html

__all__ = ["render_safe_refactor_html", "render_schema_drift_html"]
