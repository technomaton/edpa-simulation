#!/usr/bin/env python3
"""
EDPA Monte Carlo CW Calibration -- 1000+ Random Team Compositions

Generates N random team scenarios (default 1000), simulates realistic
contribution patterns with ground truth corrections, and derives
statistically robust CW weights with confidence intervals.

Extends the 8-scenario calibrate_roles.py approach to a full Monte Carlo
simulation for higher confidence in recommended heuristic weights.

Usage:
    python scripts/monte_carlo_calibration.py
    python scripts/monte_carlo_calibration.py --scenarios 2000 --seed 123

Outputs:
    - Full report to stdout
    - data/monte_carlo_report.json          -- raw data for audit
    - config/cw_heuristics_monte_carlo.yaml -- recommended weights with confidence
"""

import argparse
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML required. Install with: pip install pyyaml")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"

# ---------------------------------------------------------------------------
# Current CW heuristic defaults (from config/cw_heuristics.yaml)
# ---------------------------------------------------------------------------

CURRENT_ROLE_WEIGHTS = {
    "owner": 1.0,
    "key": 0.6,
    "reviewer": 0.25,
    "consulted": 0.15,
}

SIGNAL_MAP = {
    "assignee": ("owner", 1.0),
    "pr_author": ("key", 0.6),
    "commit_author": ("reviewer", 0.25),
    "issue_comment": ("consulted", 0.15),
}

# ---------------------------------------------------------------------------
# Role pool and team generation
# ---------------------------------------------------------------------------

ALL_ROLES = ["PM", "BO", "Arch", "Dev", "DevSecOps", "QA"]

FTE_OPTIONS = [0.25, 0.5, 0.75, 1.0, 1.2]
FTE_WEIGHTS = [0.05, 0.10, 0.15, 0.55, 0.15]  # weighted toward 1.0

JOB_SIZES = [1, 2, 3, 5, 8]
JOB_SIZE_WEIGHTS = [0.08, 0.15, 0.32, 0.30, 0.15]  # weighted toward 3 and 5

STORY_TITLES = [
    "REST API endpoint", "Database migration", "Unit test module",
    "Integration tests", "CI/CD pipeline config", "Monitoring dashboards",
    "Authorization model", "Cache layer", "ETL transformation",
    "OpenAPI specification", "Input validation", "Audit log service",
    "CSV data export", "Notification system", "Environment config",
    "Security scan", "Performance benchmark", "Load testing suite",
    "API documentation", "Core refactoring", "Retry logic",
    "Event sourcing module", "GraphQL resolver", "Webhook handler",
    "Rate limiting service", "Health check endpoint", "Batch processing",
    "Report generator", "Email templates", "Full-text search",
]

# ---------------------------------------------------------------------------
# Correction patterns: (role, evidence_role) -> (probability, direction, values)
# Extended and refined from calibrate_roles.py patterns.
# ---------------------------------------------------------------------------

CORRECTION_PATTERNS = {
    # BO: Rarely commits. Auto-detects as consulted (0.15).
    # Team confirms strategic decisions merit 0.25-0.50.
    ("BO", "consulted"):  (0.75, "up", [0.25, 0.30, 0.35, 0.40, 0.50]),
    ("BO", "reviewer"):   (0.60, "up", [0.35, 0.40, 0.50]),

    # PM: Some commits (specs, docs). Stakeholder management invisible to Git.
    ("PM", "consulted"):  (0.60, "up", [0.20, 0.25, 0.30, 0.40, 0.50]),
    ("PM", "reviewer"):   (0.45, "up", [0.30, 0.35, 0.40]),
    ("PM", "key"):        (0.20, "up", [0.65, 0.70]),

    # Arch: Reviews everything, few own commits. Design decisions invisible.
    ("Arch", "reviewer"):  (0.55, "up", [0.30, 0.35, 0.40, 0.50, 0.60]),
    ("Arch", "consulted"): (0.50, "up", [0.25, 0.30, 0.35, 0.40]),
    ("Arch", "owner"):     (0.10, "down", [0.80, 0.90]),
    ("Arch", "key"):       (0.15, "up", [0.65, 0.70]),

    # Dev (owner): Many commits, assignee. Usually correct.
    ("Dev", "owner"):     (0.05, "down", [0.80, 0.90]),
    # Dev (contributor): Pair programming adjustments.
    ("Dev", "key"):       (0.15, "up", [0.65, 0.70, 0.75]),
    ("Dev", "reviewer"):  (0.25, "up", [0.30, 0.35, 0.40, 0.50]),
    ("Dev", "consulted"): (0.20, "up", [0.20, 0.25, 0.30]),

    # DevSecOps: Infra work often invisible to Git signals.
    ("DevSecOps", "consulted"): (0.40, "up", [0.25, 0.30, 0.35, 0.40, 0.50]),
    ("DevSecOps", "reviewer"):  (0.40, "up", [0.30, 0.35, 0.40, 0.50]),
    ("DevSecOps", "key"):       (0.20, "up", [0.65, 0.70]),
    ("DevSecOps", "owner"):     (0.10, "down", [0.80, 0.90]),

    # QA (owner): Many test commits inflate auto-CW.
    ("QA", "owner"):     (0.30, "down", [0.60, 0.70, 0.80]),
    # QA (key): Tests don't equal feature ownership.
    ("QA", "key"):       (0.25, "down", [0.40, 0.50]),
    ("QA", "reviewer"):  (0.20, "up", [0.30, 0.35]),
    ("QA", "consulted"): (0.15, "up", [0.20, 0.25]),
}


# ---------------------------------------------------------------------------
# Team generation
# ---------------------------------------------------------------------------

def generate_team(rng, team_size):
    """Generate a random team composition respecting role constraints."""
    team = []
    member_id = 0

    def add_member(role):
        nonlocal member_id
        member_id += 1
        fte = rng.choices(FTE_OPTIONS, weights=FTE_WEIGHTS, k=1)[0]
        team.append({
            "id": f"{role.lower()}{member_id}",
            "name": f"{role}-{member_id}",
            "role": role,
            "fte": fte,
        })

    # PM: 0-2 (probability 70% for 1, 20% for 2, 10% for 0)
    pm_count = rng.choices([0, 1, 2], weights=[0.10, 0.70, 0.20], k=1)[0]
    for _ in range(pm_count):
        add_member("PM")

    # BO: 0-1 (probability 40% for 1)
    bo_count = 1 if rng.random() < 0.40 else 0
    for _ in range(bo_count):
        add_member("BO")

    # Arch: 0-2 (probability 50% for 1, 15% for 2, 35% for 0)
    arch_count = rng.choices([0, 1, 2], weights=[0.35, 0.50, 0.15], k=1)[0]
    for _ in range(arch_count):
        add_member("Arch")

    # DevSecOps: 0-2 (probability 45% for 1, 10% for 2, 45% for 0)
    dso_count = rng.choices([0, 1, 2], weights=[0.45, 0.45, 0.10], k=1)[0]
    for _ in range(dso_count):
        add_member("DevSecOps")

    # QA: 0-3 (probability 55% for 1, 20% for 2, 5% for 3, 20% for 0)
    qa_count = rng.choices([0, 1, 2, 3], weights=[0.20, 0.55, 0.20, 0.05], k=1)[0]
    for _ in range(qa_count):
        add_member("QA")

    # Dev: fill remaining slots (at least 1)
    non_dev_count = len(team)
    remaining = max(1, team_size - non_dev_count)
    # Cap devs at 6
    dev_count = min(remaining, 6)
    for _ in range(dev_count):
        add_member("Dev")

    # Ensure at least 1 Dev is present (guaranteed by max(1, ...))
    dev_ids = [m for m in team if m["role"] == "Dev"]
    if not dev_ids:
        add_member("Dev")

    return team


# ---------------------------------------------------------------------------
# Story generation per scenario
# ---------------------------------------------------------------------------

def generate_stories(team, num_stories, rng):
    """Generate work items with contributors for a random team."""
    stories = []

    devs = [m for m in team if m["role"] == "Dev"]
    archs = [m for m in team if m["role"] == "Arch"]
    dsos = [m for m in team if m["role"] == "DevSecOps"]
    qas = [m for m in team if m["role"] == "QA"]
    pms = [m for m in team if m["role"] == "PM"]
    bos = [m for m in team if m["role"] == "BO"]
    tech_pool = devs + dsos  # people who can own stories

    if not tech_pool:
        tech_pool = [m for m in team if m["role"] not in ("BO", "PM")]
    if not tech_pool:
        tech_pool = team[:]

    # Compute cross_functional and mgmt_involvement from team shape
    team_size = len(team)
    cross_functional = min(0.80, 0.30 + (15 - team_size) * 0.04) if team_size < 10 else 0.25
    mgmt_involvement = 0.30 + 0.10 * len(pms) + 0.15 * len(bos)
    infra_weight = 0.15 + 0.20 * len(dsos)

    for i in range(num_stories):
        sid = f"S-{i + 1:04d}"
        title = rng.choice(STORY_TITLES)
        js = rng.choices(JOB_SIZES, weights=JOB_SIZE_WEIGHTS, k=1)[0]

        # Assign owner: Dev or DevSecOps, weighted by FTE
        owner_weights = []
        for m in tech_pool:
            w = m["fte"]
            if m["role"] == "Dev":
                w *= 2.5
            elif m["role"] == "DevSecOps":
                w *= 1.0 + infra_weight
            else:
                w *= 0.6
            owner_weights.append(w)

        owner = rng.choices(tech_pool, weights=owner_weights, k=1)[0]

        # Determine number of contributors (1-4 based on team size)
        max_contributors = min(4, team_size - 1)
        if max_contributors < 1:
            max_contributors = 1
        target_contributors = rng.randint(1, max_contributors)

        contributors = []
        other_members = [m for m in team if m["id"] != owner["id"]]

        # Dev contributors (peer review, pair programming)
        for m in other_members:
            if m["role"] == "Dev":
                p = 0.35 + cross_functional * 0.25
                if rng.random() < p:
                    contributors.append(m)

        # Arch involvement
        for m in other_members:
            if m["role"] == "Arch":
                p = 0.25 + (0.15 if js >= 5 else 0.0)
                if rng.random() < p:
                    contributors.append(m)

        # DevSecOps involvement
        for m in other_members:
            if m["role"] == "DevSecOps":
                p = 0.20 + infra_weight * 0.30
                if rng.random() < p:
                    contributors.append(m)

        # QA involvement
        for m in other_members:
            if m["role"] == "QA":
                p = 0.30 + (0.10 if js >= 5 else 0.0)
                if rng.random() < p:
                    contributors.append(m)

        # PM/BO peripheral involvement
        for m in other_members:
            if m["role"] == "PM":
                p = 0.15 + mgmt_involvement * 0.30
                if rng.random() < p:
                    contributors.append(m)
            elif m["role"] == "BO":
                p = 0.10 + mgmt_involvement * 0.20
                if rng.random() < p:
                    contributors.append(m)

        # Deduplicate and cap at target
        seen = set()
        unique_contributors = []
        for c in contributors:
            if c["id"] not in seen:
                seen.add(c["id"])
                unique_contributors.append(c)
        if len(unique_contributors) > target_contributors:
            unique_contributors = rng.sample(unique_contributors, target_contributors)

        stories.append({
            "id": sid,
            "title": title,
            "js": js,
            "owner": owner,
            "contributors": unique_contributors,
        })

    return stories


# ---------------------------------------------------------------------------
# Signal detection and CW computation
# ---------------------------------------------------------------------------

def compute_auto_cw_and_evidence(person, story, rng):
    """
    Determine auto_cw and evidence_role based on the person's relationship
    to the story, simulating what Git signal detection would produce.

    Returns: (evidence_role, auto_cw, primary_signal)
    """
    pid = person["id"]
    role = person["role"]
    owner = story["owner"]
    contributor_ids = [c["id"] for c in story["contributors"]]

    if pid == owner["id"]:
        evidence_role = "owner"
        auto_cw = CURRENT_ROLE_WEIGHTS["owner"]
        primary_signal = "assignee"
    elif pid in contributor_ids:
        if role == "Dev":
            if rng.random() < 0.70:
                evidence_role = "key"
                auto_cw = CURRENT_ROLE_WEIGHTS["key"]
                primary_signal = "pr_author"
            else:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
        elif role == "Arch":
            if rng.random() < 0.40:
                evidence_role = "key"
                auto_cw = CURRENT_ROLE_WEIGHTS["key"]
                primary_signal = "pr_author"
            else:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
        elif role == "DevSecOps":
            if rng.random() < 0.50:
                evidence_role = "key"
                auto_cw = CURRENT_ROLE_WEIGHTS["key"]
                primary_signal = "pr_author"
            else:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
        elif role == "QA":
            if rng.random() < 0.55:
                evidence_role = "key"
                auto_cw = CURRENT_ROLE_WEIGHTS["key"]
                primary_signal = "pr_author"
            else:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
        elif role in ("PM", "BO"):
            if rng.random() < 0.25:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
            else:
                evidence_role = "consulted"
                auto_cw = CURRENT_ROLE_WEIGHTS["consulted"]
                primary_signal = "issue_comment"
        else:
            evidence_role = "consulted"
            auto_cw = CURRENT_ROLE_WEIGHTS["consulted"]
            primary_signal = "issue_comment"
    else:
        # Peripheral involvement (reviews, comments)
        if role in ("Arch", "QA"):
            evidence_role = "reviewer"
            auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
            primary_signal = "commit_author"
        else:
            evidence_role = "consulted"
            auto_cw = CURRENT_ROLE_WEIGHTS["consulted"]
            primary_signal = "issue_comment"

    return evidence_role, auto_cw, primary_signal


def apply_correction(role, evidence_role, auto_cw, rng):
    """Apply realistic team retro correction to auto_cw."""
    confirmed_cw = auto_cw
    was_corrected = False

    correction_key = (role, evidence_role)
    if correction_key in CORRECTION_PATTERNS:
        prob, direction, values = CORRECTION_PATTERNS[correction_key]
        if rng.random() < prob:
            corrected = rng.choice(values)
            if direction == "up":
                confirmed_cw = max(auto_cw, corrected)
            else:
                confirmed_cw = min(auto_cw, corrected)
            if abs(confirmed_cw - auto_cw) > 0.001:
                was_corrected = True

    return round(confirmed_cw, 2), was_corrected


# ---------------------------------------------------------------------------
# Scenario simulation
# ---------------------------------------------------------------------------

def simulate_scenario(scenario_id, rng):
    """
    Simulate a single randomly generated scenario.

    Returns: (records, team, stories)
    """
    # Random team size: 4-15 uniform
    team_size = rng.randint(4, 15)
    team = generate_team(rng, team_size)

    # Random number of stories: 10-30
    num_stories = rng.randint(10, 30)
    stories = generate_stories(team, num_stories, rng)

    team_by_id = {m["id"]: m for m in team}
    records = []

    for story in stories:
        # Collect all involved people (owner + contributors + peripheral)
        involved = set()
        involved.add(story["owner"]["id"])
        for c in story["contributors"]:
            involved.add(c["id"])

        # Add peripheral involvement
        for m in team:
            if m["id"] in involved:
                continue
            role = m["role"]
            if role == "QA" and rng.random() < 0.25:
                involved.add(m["id"])
            elif role == "Arch" and story["js"] >= 5 and rng.random() < 0.20:
                involved.add(m["id"])
            elif role == "PM" and rng.random() < 0.15:
                involved.add(m["id"])
            elif role == "BO" and rng.random() < 0.10:
                involved.add(m["id"])

        for pid in involved:
            person = team_by_id[pid]
            evidence_role, auto_cw, primary_signal = compute_auto_cw_and_evidence(
                person, story, rng
            )
            confirmed_cw, was_corrected = apply_correction(
                person["role"], evidence_role, auto_cw, rng
            )
            deviation = abs(confirmed_cw - auto_cw)

            records.append({
                "scenario_id": scenario_id,
                "person_id": pid,
                "person_role": person["role"],
                "item_id": story["id"],
                "item_js": story["js"],
                "evidence_role": evidence_role,
                "primary_signal": primary_signal,
                "auto_cw": round(auto_cw, 2),
                "confirmed_cw": confirmed_cw,
                "deviation": round(deviation, 3),
                "was_corrected": was_corrected,
            })

    return records, team, stories


# ---------------------------------------------------------------------------
# Statistical analysis
# ---------------------------------------------------------------------------

def compute_percentiles(values, percentiles):
    """Compute percentiles from a sorted list of values."""
    if not values:
        return {p: 0.0 for p in percentiles}
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    result = {}
    for p in percentiles:
        k = (p / 100.0) * (n - 1)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            result[p] = sorted_vals[int(k)]
        else:
            result[p] = sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)
    return result


def t_test_one_sample(values, mu0=0.0):
    """
    Simple one-sample t-test: is the mean of values significantly different
    from mu0?

    Returns: (t_statistic, p_value_approx)
    Uses a rough two-tailed p-value approximation.
    """
    n = len(values)
    if n < 2:
        return 0.0, 1.0

    mean_val = statistics.mean(values)
    std_val = statistics.stdev(values)

    if std_val < 1e-12:
        # All values identical
        if abs(mean_val - mu0) < 1e-12:
            return 0.0, 1.0
        else:
            return float("inf"), 0.0

    se = std_val / math.sqrt(n)
    t_stat = (mean_val - mu0) / se

    # Approximate two-tailed p-value using the formula:
    # p ~ 2 * (1 - Phi(|t| * sqrt(1 - 1/(4*df))))
    # where Phi is the standard normal CDF approximated via error function
    df = n - 1
    z = abs(t_stat) * math.sqrt(1.0 - 1.0 / (4.0 * df)) if df >= 1 else abs(t_stat)

    # Approximate normal CDF using math.erfc
    p_value = math.erfc(z / math.sqrt(2.0))
    return t_stat, min(p_value, 1.0)


def compute_role_statistics(all_records):
    """Compute per-(role, evidence_role) statistics across all scenarios."""
    buckets = {}

    for rec in all_records:
        key = (rec["person_role"], rec["evidence_role"])
        buckets.setdefault(key, []).append(rec)

    role_stats = {}
    for (role, ev_role), recs in sorted(buckets.items()):
        auto_vals = [r["auto_cw"] for r in recs]
        confirmed_vals = [r["confirmed_cw"] for r in recs]
        bias_vals = [r["confirmed_cw"] - r["auto_cw"] for r in recs]
        corrected_count = sum(1 for r in recs if r["was_corrected"])

        n = len(recs)
        avg_auto = statistics.mean(auto_vals)
        avg_confirmed = statistics.mean(confirmed_vals)
        avg_bias = statistics.mean(bias_vals)
        std_bias = statistics.stdev(bias_vals) if n > 1 else 0.0
        correction_rate = corrected_count / n * 100 if n > 0 else 0

        std_confirmed = statistics.stdev(confirmed_vals) if n > 1 else 0.0
        median_confirmed = statistics.median(confirmed_vals)

        # Percentiles
        pcts = compute_percentiles(confirmed_vals, [5, 25, 50, 75, 95])

        # 95% confidence interval for mean confirmed_cw
        se = std_confirmed / math.sqrt(n) if n > 0 else 0.0
        ci_low = avg_confirmed - 1.96 * se
        ci_high = avg_confirmed + 1.96 * se

        # T-test: is bias significantly different from 0?
        t_stat, p_value = t_test_one_sample(bias_vals, mu0=0.0)

        # Confidence level
        if n > 500 and std_confirmed < 0.15:
            confidence = "HIGH"
        elif n > 100:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        role_stats[(role, ev_role)] = {
            "n": n,
            "avg_auto": round(avg_auto, 4),
            "avg_confirmed": round(avg_confirmed, 4),
            "median_confirmed": round(median_confirmed, 4),
            "std_confirmed": round(std_confirmed, 4),
            "avg_bias": round(avg_bias, 4),
            "std_bias": round(std_bias, 4),
            "ci_low": round(ci_low, 4),
            "ci_high": round(ci_high, 4),
            "correction_rate": round(correction_rate, 1),
            "t_statistic": round(t_stat, 4),
            "p_value": round(p_value, 6),
            "confidence": confidence,
            "p5": round(pcts[5], 4),
            "p25": round(pcts[25], 4),
            "p50": round(pcts[50], 4),
            "p75": round(pcts[75], 4),
            "p95": round(pcts[95], 4),
        }

    return role_stats


def recommend_weight(median_confirmed):
    """
    Derive a recommended CW from median of confirmed values.
    Rounded to nearest 0.05 for simplicity.
    """
    return round(round(median_confirmed / 0.05) * 0.05, 2)


def compute_mad(records, weights_func):
    """Compute MAD using a weight lookup function."""
    if not records:
        return 0.0
    total_dev = 0.0
    for rec in records:
        w = weights_func(rec)
        total_dev += abs(w - rec["confirmed_cw"])
    return total_dev / len(records)


# ---------------------------------------------------------------------------
# Report output
# ---------------------------------------------------------------------------

def make_bar(count, total, width=20):
    """Create a simple ASCII bar."""
    frac = count / total if total > 0 else 0
    filled = int(frac * width)
    return "\u2588" * filled + " " * (width - filled)


def print_report(all_records, scenario_summaries, role_stats, recommendations,
                 mad_before, mad_after, num_scenarios, elapsed_seconds):
    """Print the full Monte Carlo calibration report."""
    total_records = len(all_records)

    # Team size distribution
    team_sizes = [s["team_size"] for s in scenario_summaries]
    size_buckets = {"4-6": 0, "7-9": 0, "10-12": 0, "13-15": 0}
    for ts in team_sizes:
        if ts <= 6:
            size_buckets["4-6"] += 1
        elif ts <= 9:
            size_buckets["7-9"] += 1
        elif ts <= 12:
            size_buckets["10-12"] += 1
        else:
            size_buckets["13-15"] += 1

    print()
    print("\u2550" * 75)
    print("  MONTE CARLO CW CALIBRATION -- {} SCENARIOS".format(num_scenarios))
    print("  Total records: {:,}".format(total_records))
    print("  Runtime: {:.1f}s".format(elapsed_seconds))
    print("\u2550" * 75)
    print()

    # Team size distribution
    print("  Distribution of team sizes:")
    for label, count in size_buckets.items():
        pct = count / num_scenarios * 100
        bar = make_bar(count, num_scenarios, width=25)
        print("    {:<6} {} {:>4} ({:.1f}%)".format(label, bar, count, pct))
    print()

    # Story count distribution
    story_counts = [s["num_stories"] for s in scenario_summaries]
    avg_stories = statistics.mean(story_counts)
    min_stories = min(story_counts)
    max_stories = max(story_counts)
    print("  Stories per scenario:  min={}, avg={:.1f}, max={}".format(
        min_stories, avg_stories, max_stories))

    # Records per scenario
    rec_counts = [s["num_records"] for s in scenario_summaries]
    avg_recs = statistics.mean(rec_counts)
    print("  Records per scenario:  min={}, avg={:.1f}, max={}".format(
        min(rec_counts), avg_recs, max(rec_counts)))
    print()

    # Role distribution across all scenarios
    print("  Role distribution across all scenarios:")
    role_counts = {}
    for s in scenario_summaries:
        for role, count in s["role_counts"].items():
            role_counts.setdefault(role, []).append(count)
    for role in ["BO", "PM", "Arch", "Dev", "DevSecOps", "QA"]:
        if role in role_counts:
            vals = role_counts[role]
            total = sum(vals)
            avg = total / num_scenarios
            present = sum(1 for v in vals if v > 0)
            print("    {:<10}  present in {:.1f}% of teams, avg count: {:.2f}".format(
                role, present / num_scenarios * 100, avg))
    print()

    # Main calibration table
    print("\u2500" * 75)
    print("  CALIBRATION RESULTS")
    print("\u2500" * 75)
    print("  {:<10} {:<10} {:>8} {:>7} {:>7} {:>7} {:>6} {:>14} {:>6} {:>6}".format(
        "Role", "Evidence", "N(total)", "Auto", "Median", "Mean", "Std",
        "95%CI", "Recomm", "Conf"))
    print("  " + "\u2500" * 73)

    for (role, ev_role) in sorted(role_stats.keys()):
        stats = role_stats[(role, ev_role)]
        rec = recommendations.get((role, ev_role), stats["median_confirmed"])
        ci_str = "[{:.2f},{:.2f}]".format(stats["ci_low"], stats["ci_high"])
        print("  {:<10} {:<10} {:>8,} {:>7.2f} {:>7.2f} {:>7.2f} {:>6.3f} {:>14} {:>6.2f} {:>6}".format(
            role, ev_role, stats["n"], stats["avg_auto"],
            stats["median_confirmed"], stats["avg_confirmed"],
            stats["std_confirmed"], ci_str, rec, stats["confidence"]))

    print()

    # Detailed bias analysis
    print("\u2500" * 75)
    print("  BIAS ANALYSIS (confirmed - auto)")
    print("\u2500" * 75)
    print("  {:<10} {:<10} {:>8} {:>8} {:>8} {:>10} {:>8} {:>10}".format(
        "Role", "Evidence", "Bias", "StdBias", "CorrRate", "t-stat", "p-value", "Signif"))
    print("  " + "\u2500" * 73)

    for (role, ev_role) in sorted(role_stats.keys()):
        stats = role_stats[(role, ev_role)]
        sig = "***" if stats["p_value"] < 0.001 else (
            "**" if stats["p_value"] < 0.01 else (
                "*" if stats["p_value"] < 0.05 else "n.s."))
        print("  {:<10} {:<10} {:>+8.4f} {:>8.4f} {:>7.1f}% {:>10.2f} {:>8.4f} {:>10}".format(
            role, ev_role, stats["avg_bias"], stats["std_bias"],
            stats["correction_rate"], stats["t_statistic"],
            stats["p_value"], sig))

    print()

    # Percentile distribution
    print("\u2500" * 75)
    print("  PERCENTILE DISTRIBUTION (confirmed_cw)")
    print("\u2500" * 75)
    print("  {:<10} {:<10} {:>7} {:>7} {:>7} {:>7} {:>7}".format(
        "Role", "Evidence", "p5", "p25", "p50", "p75", "p95"))
    print("  " + "\u2500" * 55)

    for (role, ev_role) in sorted(role_stats.keys()):
        stats = role_stats[(role, ev_role)]
        print("  {:<10} {:<10} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f}".format(
            role, ev_role, stats["p5"], stats["p25"], stats["p50"],
            stats["p75"], stats["p95"]))

    print()

    # Comparison with current heuristics
    print("\u2500" * 75)
    print("  COMPARISON WITH CURRENT HEURISTICS")
    print("\u2500" * 75)
    print("  {:<10} {:<10} {:>9} {:>9} {:>9}".format(
        "Role", "Evidence", "Current", "MC Recomm", "Change"))
    print("  " + "\u2500" * 47)

    for (role, ev_role) in sorted(recommendations.keys()):
        current = CURRENT_ROLE_WEIGHTS.get(ev_role, 0.15)
        rec = recommendations[(role, ev_role)]
        change = rec - current
        change_str = "{:+.2f}".format(change) if abs(change) > 0.001 else " 0.00"
        print("  {:<10} {:<10} {:>9.2f} {:>9.2f} {:>9}".format(
            role, ev_role, current, rec, change_str))

    print()

    # MAD summary
    print("\u2500" * 75)
    print("  MAD ANALYSIS")
    print("\u2500" * 75)
    mad_reduction = ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0
    print("    Original MAD:      {:.6f}".format(mad_before))
    print("    Monte Carlo MAD:   {:.6f}".format(mad_after))
    print("    Improvement:       {:.1f}%".format(mad_reduction))
    print()

    print("\u2550" * 75)
    print("  Calibration complete. Output files:")
    print("    config/cw_heuristics_monte_carlo.yaml")
    print("    data/monte_carlo_report.json")
    print("\u2550" * 75)
    print()


# ---------------------------------------------------------------------------
# YAML/JSON output
# ---------------------------------------------------------------------------

def save_monte_carlo_heuristics(role_stats, recommendations, mad_before, mad_after,
                                num_scenarios, total_records):
    """Save recommended heuristics YAML with confidence levels."""
    # Build single-value role_weights (cross-role average per evidence_role)
    ev_role_values = {}
    for (role, ev_role), weight in recommendations.items():
        ev_role_values.setdefault(ev_role, []).append((role, weight))

    calibrated_role_weights = dict(CURRENT_ROLE_WEIGHTS)
    for ev_role, entries in ev_role_values.items():
        if len(entries) == 1:
            calibrated_role_weights[ev_role] = entries[0][1]
        else:
            avg = statistics.mean(w for _, w in entries)
            calibrated_role_weights[ev_role] = round(round(avg / 0.05) * 0.05, 2)

    # Build role_overrides with confidence
    role_overrides = {}
    for (team_role, ev_role), weight in sorted(recommendations.items()):
        stats = role_stats.get((team_role, ev_role), {})
        entry = {"weight": weight}
        if stats:
            entry["confidence"] = stats.get("confidence", "LOW")
            entry["n"] = stats.get("n", 0)
            entry["std"] = stats.get("std_confirmed", 0)
        role_overrides.setdefault(team_role, {})[ev_role] = entry

    # Flatten for simpler YAML: just weight values in role_overrides_simple
    role_overrides_simple = {}
    for team_role, ev_map in role_overrides.items():
        role_overrides_simple[team_role] = {
            ev: info["weight"] for ev, info in ev_map.items()
        }

    mad_reduction = ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0

    output = {
        "role_weights": calibrated_role_weights,
        "role_overrides": role_overrides_simple,
        "role_overrides_detailed": {
            team_role: {
                ev: {
                    "weight": info["weight"],
                    "confidence": info.get("confidence", "LOW"),
                    "sample_size": info.get("n", 0),
                    "std_dev": info.get("std", 0),
                }
                for ev, info in ev_map.items()
            }
            for team_role, ev_map in role_overrides.items()
        },
        "signal_weights": {
            "assignee": 4.0,
            "contribute_command": 3.0,
            "pr_author": 2.0,
            "commit_author": 1.0,
            "pr_reviewer": 1.0,
            "issue_comment": 0.5,
        },
        "rules": {
            "highest_signal_wins": True,
            "manual_override_priority": True,
            "commit_count_is_not_time": True,
            "in_progress_items_excluded": True,
        },
        "calibration": {
            "method": "Monte Carlo statistical calibration",
            "scenarios": num_scenarios,
            "total_records": total_records,
            "mad_before": round(mad_before, 6),
            "mad_after": round(mad_after, 6),
            "mad_reduction_pct": round(mad_reduction, 1),
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    path = CONFIG_DIR / "cw_heuristics_monte_carlo.yaml"
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("# EDPA CW Heuristics -- Monte Carlo Calibration\n")
        f.write("# Generated: {}\n".format(
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
        f.write("# Scenarios: {}, Records: {:,}\n".format(num_scenarios, total_records))
        f.write("# MAD reduction: {}%\n".format(output["calibration"]["mad_reduction_pct"]))
        f.write("#\n")
        f.write("# role_weights: single-value defaults (backward compatible)\n")
        f.write("# role_overrides: per-team-role weights (simple)\n")
        f.write("# role_overrides_detailed: weights with confidence and sample size\n")
        f.write("\n")
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    return path


def save_monte_carlo_report(all_records, scenario_summaries, role_stats,
                            recommendations, mad_before, mad_after,
                            num_scenarios, elapsed_seconds):
    """Save detailed Monte Carlo report as JSON."""
    # Do not save individual records in JSON (would be huge).
    # Save aggregated stats instead.

    report = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "methodology": "EDPA v2.2 Monte Carlo CW calibration",
            "scenarios_count": num_scenarios,
            "total_records": len(all_records),
            "runtime_seconds": round(elapsed_seconds, 2),
        },
        "mad_summary": {
            "before": round(mad_before, 6),
            "after": round(mad_after, 6),
            "reduction_pct": round(
                ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0, 1
            ),
        },
        "scenario_distribution": {
            "team_sizes": {
                "min": min(s["team_size"] for s in scenario_summaries),
                "max": max(s["team_size"] for s in scenario_summaries),
                "mean": round(statistics.mean(s["team_size"] for s in scenario_summaries), 2),
                "std": round(statistics.stdev(s["team_size"] for s in scenario_summaries), 2)
                    if len(scenario_summaries) > 1 else 0.0,
            },
            "stories_per_scenario": {
                "min": min(s["num_stories"] for s in scenario_summaries),
                "max": max(s["num_stories"] for s in scenario_summaries),
                "mean": round(statistics.mean(s["num_stories"] for s in scenario_summaries), 2),
            },
            "records_per_scenario": {
                "min": min(s["num_records"] for s in scenario_summaries),
                "max": max(s["num_records"] for s in scenario_summaries),
                "mean": round(statistics.mean(s["num_records"] for s in scenario_summaries), 2),
            },
        },
        "role_statistics": {
            "{}|{}".format(role, ev_role): {
                **stats,
                "recommended_cw": recommendations.get((role, ev_role), stats["median_confirmed"]),
            }
            for (role, ev_role), stats in sorted(role_stats.items())
        },
        "recommendations": {
            "{}|{}".format(role, ev_role): weight
            for (role, ev_role), weight in sorted(recommendations.items())
        },
        "current_defaults": CURRENT_ROLE_WEIGHTS,
    }

    path = DATA_DIR / "monte_carlo_report.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="EDPA Monte Carlo CW Calibration -- random team scenarios"
    )
    parser.add_argument("--scenarios", type=int, default=1000,
                        help="Number of random scenarios (default: 1000)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    num_scenarios = args.scenarios
    seed = args.seed
    rng = random.Random(seed)

    print()
    print("  EDPA Monte Carlo CW Calibration")
    print("  " + "=" * 50)
    print("  Scenarios: {:,}   Seed: {}".format(num_scenarios, seed))
    print("  Generating random team compositions...\n")

    start_time = time.time()

    all_records = []
    scenario_summaries = []

    for i in range(num_scenarios):
        scenario_id = i + 1
        records, team, stories = simulate_scenario(scenario_id, rng)

        # Role counts for this team
        role_counts = {}
        for m in team:
            role_counts[m["role"]] = role_counts.get(m["role"], 0) + 1

        scenario_summaries.append({
            "scenario_id": scenario_id,
            "team_size": len(team),
            "num_stories": len(stories),
            "num_records": len(records),
            "role_counts": role_counts,
        })

        all_records.extend(records)

        # Progress every 100 scenarios
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            print("  [{:>5}/{:>5}] {:>7,} records  ({:.1f}s)".format(
                i + 1, num_scenarios, len(all_records), elapsed))

    elapsed = time.time() - start_time
    print()
    print("  Simulation complete: {:,} records in {:.1f}s".format(
        len(all_records), elapsed))
    print("  Computing statistics...")

    # Compute role statistics
    role_stats = compute_role_statistics(all_records)

    # Derive recommendations using median (more robust than mean)
    recommendations = {}
    for (role, ev_role), stats in role_stats.items():
        rec = recommend_weight(stats["median_confirmed"])
        recommendations[(role, ev_role)] = rec

    # Build MAD lookup functions
    # Per-(role, evidence_role) calibrated lookup
    role_ev_lookup = dict(recommendations)

    # Fallback: average per evidence_role
    ev_role_avgs = {}
    for (role, ev_role), weight in recommendations.items():
        ev_role_avgs.setdefault(ev_role, []).append(weight)
    calibrated_single = {}
    for ev_role, weights in ev_role_avgs.items():
        calibrated_single[ev_role] = round(statistics.mean(weights), 2)

    def original_weight(rec):
        return CURRENT_ROLE_WEIGHTS.get(rec["evidence_role"], 0.15)

    def calibrated_weight(rec):
        return role_ev_lookup.get(
            (rec["person_role"], rec["evidence_role"]),
            calibrated_single.get(rec["evidence_role"], 0.15)
        )

    mad_before = compute_mad(all_records, original_weight)
    mad_after = compute_mad(all_records, calibrated_weight)

    # Print report
    print_report(all_records, scenario_summaries, role_stats, recommendations,
                 mad_before, mad_after, num_scenarios, elapsed)

    # Save outputs
    yaml_path = save_monte_carlo_heuristics(
        role_stats, recommendations, mad_before, mad_after,
        num_scenarios, len(all_records))
    json_path = save_monte_carlo_report(
        all_records, scenario_summaries, role_stats, recommendations,
        mad_before, mad_after, num_scenarios, elapsed)

    print("  Files saved:")
    print("    {}".format(yaml_path.relative_to(ROOT)))
    print("    {}".format(json_path.relative_to(ROOT)))
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
