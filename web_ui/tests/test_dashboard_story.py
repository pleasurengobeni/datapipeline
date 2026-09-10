"""
Tests for the monitoring dashboard's "tell me the story" helpers.

data_pipeline_monitor/dashboard.py calls st.set_page_config() at import
time, so it can't be imported in a test process. These helpers are pure
functions, so they're extracted from the source and exec'd in an isolated
namespace — same pattern test_hybrid_pipeline.py uses for the DAG templates.

What's covered is the judgement these functions encode, not their plumbing:
whether a percentage is stated honestly, and whether a table gets flagged as
"off" against its own loading rhythm rather than a blanket rule.

Run with:  pytest web_ui/tests/test_dashboard_story.py -v
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

DASHBOARD = (
    Path(__file__).resolve().parent.parent.parent
    / "data_pipeline_monitor" / "dashboard.py"
)

EXTRACT = ["_delta_vs", "_humanize_hours", "_table_signals", "_format_business_date"]


@pytest.fixture(scope="module")
def helpers():
    src = DASHBOARD.read_text()
    ns = {"pd": pd}
    for name in EXTRACT:
        m = re.search(rf"^(def {name}\b.*?)(?=\ndef )", src, re.DOTALL | re.MULTILINE)
        assert m, f"{name} not found in dashboard.py — did it get renamed?"
        exec(compile(m.group(1), str(DASHBOARD), "exec"), ns)  # noqa: S102
    return ns


# ─────────────────────────────────────────────────────────────────────────────
# _delta_vs — the "vs yesterday / vs last week" comparison
# ─────────────────────────────────────────────────────────────────────────────

class TestDeltaVs:
    def test_states_a_drop_without_overstating_it(self, helpers):
        """5 rows against 1,683 is a severe drop but it is NOT a total stop.
        Rounding it to -100% would tell the reader nothing loaded at all."""
        delta, _ = helpers["_delta_vs"](5, 1683, "yesterday")
        assert delta == "-99.7% vs yesterday"

    def test_total_stop_really_does_say_100(self, helpers):
        delta, _ = helpers["_delta_vs"](0, 1683, "yesterday")
        assert delta == "-100% vs yesterday"

    def test_tiny_change_does_not_render_as_zero(self, helpers):
        """"-0%" reads as a broken widget rather than as "barely moved"."""
        delta, _ = helpers["_delta_vs"](999, 1000, "yesterday")
        assert delta == "-0.1% vs yesterday"

    def test_growth_is_positive_signed_for_green(self, helpers):
        # st.metric colours on the leading sign, so this drives the arrow.
        delta, _ = helpers["_delta_vs"](1200, 1000, "yesterday")
        assert delta.startswith("+")

    def test_no_percentage_when_there_is_no_baseline(self, helpers):
        """Going from nothing to something isn't "+infinity%" — it's the job
        starting to load. A percentage there would be noise."""
        delta, caption = helpers["_delta_vs"](500, 0, "yesterday")
        assert delta is None
        assert "new activity" in caption

    def test_quiet_on_both_sides_is_not_an_alarm(self, helpers):
        delta, caption = helpers["_delta_vs"](0, 0, "yesterday")
        assert delta is None
        assert caption == "none yesterday either"

    def test_caption_carries_the_absolute_comparison(self, helpers):
        _, caption = helpers["_delta_vs"](5, 1683, "yesterday")
        assert caption == "1,683 yesterday"


# ─────────────────────────────────────────────────────────────────────────────
# _table_signals — "what's off", judged per table
# ─────────────────────────────────────────────────────────────────────────────

class TestTableSignals:
    def _daily(self, rows):
        return pd.DataFrame(rows, columns=["table_label", "load_date", "rows_loaded"])

    def _days(self, n):
        return [d.date() for d in pd.date_range("2026-08-23", periods=n)]

    def test_healthy_table_is_not_flagged(self, helpers):
        summary = pd.DataFrame([{
            "table_label": "dw.orders", "hours_since_last_load": 1.0, "loaded_today": 1000,
        }])
        daily = self._daily([("dw.orders", d, 1000) for d in self._days(10)])
        assert helpers["_table_signals"](summary, daily).empty

    def test_weekly_table_not_flagged_for_nothing_today(self, helpers):
        """The whole point of judging per table: a fortnightly or weekly job
        having no rows today is normal, and flagging it trains people to
        ignore the page."""
        summary = pd.DataFrame([{
            "table_label": "dw.monthly_roll", "hours_since_last_load": 2.0, "loaded_today": 0,
        }])
        days = self._days(14)
        daily = self._daily([("dw.monthly_roll", days[0], 5000),
                             ("dw.monthly_roll", days[7], 5000)])
        assert helpers["_table_signals"](summary, daily).empty

    def test_daily_table_silent_today_is_flagged(self, helpers):
        summary = pd.DataFrame([{
            "table_label": "dw.orders", "hours_since_last_load": 2.0, "loaded_today": 0,
        }])
        daily = self._daily([("dw.orders", d, 1000) for d in self._days(10)])
        out = helpers["_table_signals"](summary, daily)
        assert list(out["What's off"]) == ["Nothing today"]

    def test_sharp_volume_drop_is_flagged(self, helpers):
        summary = pd.DataFrame([{
            "table_label": "dw.orders", "hours_since_last_load": 1.0, "loaded_today": 10,
        }])
        daily = self._daily([("dw.orders", d, 1000) for d in self._days(10)])
        out = helpers["_table_signals"](summary, daily)
        assert list(out["What's off"]) == ["Volume down"]
        assert "typical" in out.iloc[0]["Detail"]

    def test_stale_beats_the_volume_checks(self, helpers):
        """A table that hasn't loaded in days should say so plainly, not
        report it as a volume dip."""
        summary = pd.DataFrame([{
            "table_label": "dw.orders", "hours_since_last_load": 100.0, "loaded_today": 0,
        }])
        daily = self._daily([("dw.orders", d, 1000) for d in self._days(10)])
        out = helpers["_table_signals"](summary, daily)
        assert list(out["What's off"]) == ["Not loading"]
        assert "4 days" in out.iloc[0]["Detail"]

    def test_no_history_no_false_alarm(self, helpers):
        """A brand-new table has no rhythm to be measured against yet."""
        summary = pd.DataFrame([{
            "table_label": "dw.brand_new", "hours_since_last_load": 1.0, "loaded_today": 0,
        }])
        assert helpers["_table_signals"](summary, pd.DataFrame(
            columns=["table_label", "load_date", "rows_loaded"])).empty

    def test_empty_summary_returns_empty_frame_with_columns(self, helpers):
        out = helpers["_table_signals"](pd.DataFrame(), pd.DataFrame())
        assert out.empty
        assert list(out.columns) == ["Table", "What's off", "Detail"]


# ─────────────────────────────────────────────────────────────────────────────
# _humanize_hours / _format_business_date — presentation
# ─────────────────────────────────────────────────────────────────────────────

class TestPresentation:
    @pytest.mark.parametrize("hrs,expected", [
        (0.4, "less than an hour"), (1, "1 hour"), (5, "5 hours"),
        (47, "47 hours"), (49, "2 days"), (78, "3 days"),
    ])
    def test_humanize_hours(self, helpers, hrs, expected):
        assert helpers["_humanize_hours"](hrs) == expected

    def test_humanize_hours_survives_junk(self, helpers):
        assert helpers["_humanize_hours"](None) == "an unknown time"

    @pytest.mark.parametrize("value,expected", [
        (1816826400000, "2027-07-29"),   # epoch ms — how ArcGIS lands dates
        (1816826400,    "2027-07-29"),   # epoch seconds
        ("2024-03-15",  "2024-03-15"),
        ("2024-03-15 10:30:00", "2024-03-15"),
        (None, "N/A"),
    ])
    def test_format_business_date(self, helpers, value, expected):
        assert helpers["_format_business_date"](value) == expected

    def test_bare_year_is_not_read_as_a_1970_epoch(self, helpers):
        assert helpers["_format_business_date"](2024) == "2024"

    def test_unparseable_text_is_shown_as_is(self, helpers):
        assert helpers["_format_business_date"]("not-a-date") == "not-a-date"
