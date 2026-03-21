#!/usr/bin/env python3
"""
EDPA Multi-Scenario CW Calibration -- Role-Optimized Defaults

Runs 8 diverse team composition scenarios, simulates realistic contribution
patterns with ground truth corrections, and derives statistically optimal
CW default weights per (role, evidence_role) pair.

Usage:
    python scripts/calibrate_roles.py

Outputs:
    - Full report to stdout
    - config/cw_heuristics_calibrated.yaml  -- recommended heuristic values
    - data/calibration_report.json          -- all raw data for audit
"""

import json
import math
import os
import random
import statistics
import sys
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
# Scenario definitions
# ---------------------------------------------------------------------------

SCENARIOS = [
    {
        "id": 1,
        "name": "Startup",
        "description": "Malý tým, každý dělá všechno",
        "seed": 1001,
        "team": [
            {"id": "pm1", "name": "Anna Procházková", "role": "PM", "fte": 0.60},
            {"id": "arch1", "name": "Lukáš Dvořák", "role": "Arch", "fte": 0.80},
            {"id": "dev1", "name": "Martin Horák", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Jana Krejčí", "role": "Dev", "fte": 1.00},
            {"id": "qa1", "name": "Eva Malá", "role": "QA", "fte": 0.80},
        ],
        "characteristics": {
            "cross_functional": 0.70,   # high: everyone touches everything
            "mgmt_involvement": 0.50,
            "infra_weight": 0.20,
        },
    },
    {
        "id": 2,
        "name": "Enterprise",
        "description": "Velký specializovaný tým",
        "seed": 2002,
        "team": [
            {"id": "bo1", "name": "Vladimír Novotný", "role": "BO", "fte": 0.30},
            {"id": "pm1", "name": "Petra Šťastná", "role": "PM", "fte": 0.50},
            {"id": "pm2", "name": "Jakub Veselý", "role": "PM", "fte": 0.50},
            {"id": "arch1", "name": "Radek Fiala", "role": "Arch", "fte": 0.70},
            {"id": "arch2", "name": "Simona Pokorná", "role": "Arch", "fte": 0.70},
            {"id": "dev1", "name": "David Kučera", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Jiří Marek", "role": "Dev", "fte": 1.00},
            {"id": "dev3", "name": "Tomáš Jelínek", "role": "Dev", "fte": 1.00},
            {"id": "dev4", "name": "Michal Růžička", "role": "Dev", "fte": 1.00},
            {"id": "dso1", "name": "Pavel Beneš", "role": "DevSecOps", "fte": 0.80},
            {"id": "dso2", "name": "Lenka Sedláčková", "role": "DevSecOps", "fte": 0.80},
            {"id": "qa1", "name": "Barbora Kopecká", "role": "QA", "fte": 1.00},
        ],
        "characteristics": {
            "cross_functional": 0.25,
            "mgmt_involvement": 0.60,
            "infra_weight": 0.35,
        },
    },
    {
        "id": 3,
        "name": "DevOps-heavy",
        "description": "Infrastrukturně zaměřený tým",
        "seed": 3003,
        "team": [
            {"id": "pm1", "name": "Zdeněk Bartoš", "role": "PM", "fte": 0.50},
            {"id": "arch1", "name": "Filip Holub", "role": "Arch", "fte": 0.70},
            {"id": "dev1", "name": "Roman Vlček", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Karolína Šimková", "role": "Dev", "fte": 1.00},
            {"id": "dso1", "name": "Marek Polák", "role": "DevSecOps", "fte": 1.00},
            {"id": "dso2", "name": "Nikola Urbanová", "role": "DevSecOps", "fte": 1.00},
            {"id": "qa1", "name": "Tereza Doležalová", "role": "QA", "fte": 0.80},
        ],
        "characteristics": {
            "cross_functional": 0.40,
            "mgmt_involvement": 0.30,
            "infra_weight": 0.65,
        },
    },
    {
        "id": 4,
        "name": "Research (R&D)",
        "description": "Architekturně zaměřený výzkumný tým",
        "seed": 4004,
        "team": [
            {"id": "pm1", "name": "Alena Kratochvílová", "role": "PM", "fte": 0.40},
            {"id": "arch1", "name": "Ondřej Kříž", "role": "Arch", "fte": 0.90},
            {"id": "arch2", "name": "Hana Blažková", "role": "Arch", "fte": 0.90},
            {"id": "dev1", "name": "Vojtěch Lacko", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Kristýna Havlíčková", "role": "Dev", "fte": 1.00},
            {"id": "qa1", "name": "Štěpán Janda", "role": "QA", "fte": 0.60},
        ],
        "characteristics": {
            "cross_functional": 0.55,
            "mgmt_involvement": 0.35,
            "infra_weight": 0.15,
        },
    },
    {
        "id": 5,
        "name": "Consultancy",
        "description": "Klientsky orientovaný tým, PM silná role",
        "seed": 5005,
        "team": [
            {"id": "bo1", "name": "Igor Procházka", "role": "BO", "fte": 0.40},
            {"id": "pm1", "name": "Markéta Součková", "role": "PM", "fte": 0.80},
            {"id": "arch1", "name": "Libor Kadlec", "role": "Arch", "fte": 0.60},
            {"id": "dev1", "name": "Patrik Kořínek", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Nela Forejtová", "role": "Dev", "fte": 1.00},
            {"id": "dev3", "name": "Matěj Šimek", "role": "Dev", "fte": 1.00},
            {"id": "dso1", "name": "Ivana Hrubá", "role": "DevSecOps", "fte": 0.70},
            {"id": "qa1", "name": "Daniel Kubík", "role": "QA", "fte": 0.80},
        ],
        "characteristics": {
            "cross_functional": 0.35,
            "mgmt_involvement": 0.70,
            "infra_weight": 0.25,
        },
    },
    {
        "id": 6,
        "name": "AI-Native",
        "description": "AI provádí QA, rychlé iterace",
        "seed": 6006,
        "team": [
            {"id": "pm1", "name": "Vít Hajný", "role": "PM", "fte": 0.50},
            {"id": "arch1", "name": "Adéla Tichá", "role": "Arch", "fte": 0.80},
            {"id": "dev1", "name": "Šimon Brož", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Eliška Rybářová", "role": "Dev", "fte": 1.00},
            {"id": "dso1", "name": "Dominik Pánek", "role": "DevSecOps", "fte": 0.90},
        ],
        "characteristics": {
            "cross_functional": 0.60,
            "mgmt_involvement": 0.35,
            "infra_weight": 0.45,
        },
    },
    {
        "id": 7,
        "name": "Regulated",
        "description": "Regulovaný sektor, silný compliance",
        "seed": 7007,
        "team": [
            {"id": "bo1", "name": "Miroslav Čech", "role": "BO", "fte": 0.35},
            {"id": "pm1", "name": "Renáta Pavlíková", "role": "PM", "fte": 0.50},
            {"id": "arch1", "name": "Josef Říha", "role": "Arch", "fte": 0.70},
            {"id": "dev1", "name": "Lucie Bendová", "role": "Dev", "fte": 1.00},
            {"id": "dev2", "name": "Adam Vaněk", "role": "Dev", "fte": 1.00},
            {"id": "dso1", "name": "Klára Nováková", "role": "DevSecOps", "fte": 0.80},
            {"id": "dso2", "name": "Petr Kos", "role": "DevSecOps", "fte": 0.80},
            {"id": "qa1", "name": "Monika Zemanová", "role": "QA", "fte": 1.00},
            {"id": "qa2", "name": "Radka Fišerová", "role": "QA", "fte": 1.00},
        ],
        "characteristics": {
            "cross_functional": 0.30,
            "mgmt_involvement": 0.65,
            "infra_weight": 0.40,
        },
    },
    {
        "id": 8,
        "name": "kashealth",
        "description": "Aktuální simulace -- referenční scénář",
        "seed": 8042,
        "team": [
            {"id": "novak", "name": "Jan Novák", "role": "BO", "fte": 0.30},
            {"id": "kralova", "name": "Marie Králová", "role": "PM", "fte": 0.50},
            {"id": "urbanek", "name": "Jaroslav Urbánek", "role": "Arch", "fte": 0.70},
            {"id": "svoboda", "name": "Petr Svoboda", "role": "Dev", "fte": 1.00},
            {"id": "cerny", "name": "Tomáš Černý", "role": "Dev", "fte": 1.00},
            {"id": "tuma", "name": "Ondřej Tůma", "role": "DevSecOps", "fte": 0.80},
            {"id": "nemcova", "name": "Kateřina Němcová", "role": "QA", "fte": 1.20},
        ],
        "characteristics": {
            "cross_functional": 0.40,
            "mgmt_involvement": 0.45,
            "infra_weight": 0.30,
        },
    },
]

# ---------------------------------------------------------------------------
# Work item generation
# ---------------------------------------------------------------------------

STORY_TITLES_CZ = [
    "REST API endpoint", "Databázová migrace", "Unit testy modulu",
    "Integrační testy", "CI/CD pipeline konfigurace", "Monitoring dashboardy",
    "Autorizační model", "Cache vrstva", "ETL transformace",
    "OpenAPI specifikace", "Validace vstupních dat", "Audit log služba",
    "Export dat do CSV", "Notifikační systém", "Konfigurace prostředí",
    "Bezpečnostní scan", "Performance benchmark", "Load testing sada",
    "Dokumentace API", "Refaktoring jádra", "Retry logika",
    "Event sourcing modul", "GraphQL resolver", "Webhook handler",
    "Rate limiting služba", "Health check endpoint", "Batch processing",
    "Report generátor", "Šablona e-mailů", "Fulltextové vyhledávání",
]

JOB_SIZES = [1, 2, 3, 5, 8, 13]
JOB_SIZE_WEIGHTS = [0.05, 0.15, 0.30, 0.25, 0.20, 0.05]


def generate_stories(scenario, rng):
    """Generate 20 work items with contributors for a scenario."""
    team = scenario["team"]
    chars = scenario["characteristics"]
    stories = []

    devs = [m for m in team if m["role"] == "Dev"]
    archs = [m for m in team if m["role"] == "Arch"]
    dsos = [m for m in team if m["role"] == "DevSecOps"]
    qas = [m for m in team if m["role"] == "QA"]
    pms = [m for m in team if m["role"] == "PM"]
    bos = [m for m in team if m["role"] == "BO"]
    tech_pool = devs + archs + dsos  # people who can own stories

    if not tech_pool:
        tech_pool = [m for m in team if m["role"] != "BO"]

    for i in range(20):
        sid = f"S-{scenario['id']}{'0' * (2 - len(str(i+1)))}{i+1}"
        title = rng.choice(STORY_TITLES_CZ)
        js = rng.choices(JOB_SIZES, weights=JOB_SIZE_WEIGHTS, k=1)[0]

        # Assign owner: mostly devs, sometimes arch/dso
        owner_weights = []
        for m in tech_pool:
            w = m["fte"]
            if m["role"] == "Dev":
                w *= 2.5
            elif m["role"] == "Arch":
                w *= 0.6
            elif m["role"] == "DevSecOps":
                w *= 1.0 + chars["infra_weight"]
            owner_weights.append(w)

        owner = rng.choices(tech_pool, weights=owner_weights, k=1)[0]

        # Contributors: other team members who participate
        contributors = []
        other_members = [m for m in team if m["id"] != owner["id"]]

        # Dev contributors (peer review, pair programming)
        for m in other_members:
            if m["role"] == "Dev":
                p = 0.35 + chars["cross_functional"] * 0.25
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
                p = 0.20 + chars["infra_weight"] * 0.30
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
                p = 0.15 + chars["mgmt_involvement"] * 0.30
                if rng.random() < p:
                    contributors.append(m)
            elif m["role"] == "BO":
                p = 0.10 + chars["mgmt_involvement"] * 0.20
                if rng.random() < p:
                    contributors.append(m)

        stories.append({
            "id": sid,
            "title": title,
            "js": js,
            "owner": owner,
            "contributors": contributors,
        })

    return stories


# ---------------------------------------------------------------------------
# Signal detection and CW computation
# ---------------------------------------------------------------------------

# Correction patterns: (role, evidence_role) -> (probability, direction, values)
# Based on observed patterns across real EDPA retrospectives.
CORRECTION_PATTERNS = {
    # BO: Rarely commits. Auto-detects as consulted (0.15).
    # Team confirms strategic decisions merit 0.30-0.50.
    ("BO", "consulted"):  (0.75, "up", [0.30, 0.35, 0.40, 0.45, 0.50]),
    ("BO", "reviewer"):   (0.60, "up", [0.35, 0.40, 0.50]),

    # PM: Some commits (specs, docs). Stakeholder management invisible to Git.
    ("PM", "consulted"):  (0.60, "up", [0.25, 0.30, 0.35, 0.40, 0.50]),
    ("PM", "reviewer"):   (0.45, "up", [0.30, 0.35, 0.40]),
    ("PM", "key"):        (0.20, "up", [0.65, 0.70]),

    # Arch: Reviews everything, few own commits. Design decisions invisible.
    ("Arch", "reviewer"):  (0.55, "up", [0.35, 0.40, 0.45, 0.50, 0.60]),
    ("Arch", "consulted"): (0.50, "up", [0.25, 0.30, 0.35, 0.40]),
    ("Arch", "owner"):     (0.10, "down", [0.80, 0.90]),
    ("Arch", "key"):       (0.15, "up", [0.65, 0.70]),

    # Dev (owner): Many commits, assignee. Usually correct.
    ("Dev", "owner"):     (0.05, "down", [0.90, 0.95]),
    # Dev (contributor): Mostly correct.
    ("Dev", "key"):       (0.15, "up", [0.65, 0.70, 0.75]),
    ("Dev", "reviewer"):  (0.25, "up", [0.30, 0.35, 0.40, 0.50]),
    ("Dev", "consulted"): (0.20, "up", [0.20, 0.25, 0.30]),

    # DevSecOps: Infra work often invisible to Git signals.
    ("DevSecOps", "consulted"): (0.40, "up", [0.25, 0.30, 0.35, 0.40, 0.50]),
    ("DevSecOps", "reviewer"):  (0.35, "up", [0.30, 0.35, 0.40, 0.50]),
    ("DevSecOps", "key"):       (0.20, "up", [0.65, 0.70]),
    ("DevSecOps", "owner"):     (0.10, "down", [0.80, 0.90]),

    # QA (owner): Many test commits inflate auto-CW.
    ("QA", "owner"):     (0.30, "down", [0.60, 0.70, 0.75, 0.80]),
    # QA (key): Tests don't equal feature ownership.
    ("QA", "key"):       (0.20, "down", [0.40, 0.45, 0.50, 0.55]),
    ("QA", "reviewer"):  (0.20, "up", [0.30, 0.35]),
    ("QA", "consulted"): (0.15, "up", [0.20, 0.25]),
}


def compute_auto_cw_and_evidence(person, story, rng):
    """
    Determine auto_cw and evidence_role based on the person's relationship
    to the story, simulating what Git signal detection would produce.
    """
    pid = person["id"]
    role = person["role"]
    owner = story["owner"]
    contributor_ids = [c["id"] for c in story["contributors"]]

    if pid == owner["id"]:
        # Owner -> assignee signal -> owner CW
        evidence_role = "owner"
        auto_cw = CURRENT_ROLE_WEIGHTS["owner"]
        primary_signal = "assignee"
    elif pid in contributor_ids:
        # Contributor: what signal do they produce?
        if role == "Dev":
            # Dev contributors: PR author (key) or commit (reviewer)
            if rng.random() < 0.70:
                evidence_role = "key"
                auto_cw = CURRENT_ROLE_WEIGHTS["key"]
                primary_signal = "pr_author"
            else:
                evidence_role = "reviewer"
                auto_cw = CURRENT_ROLE_WEIGHTS["reviewer"]
                primary_signal = "commit_author"
        elif role == "Arch":
            # Arch contributors: mostly review
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
            # Management contributors: mostly consulted
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

def simulate_scenario(scenario):
    """Simulate a single team scenario, return all person-item records."""
    rng = random.Random(scenario["seed"])
    stories = generate_stories(scenario, rng)
    records = []
    team = scenario["team"]
    team_by_id = {m["id"]: m for m in team}

    for story in stories:
        # Collect all involved people (owner + contributors + peripheral)
        involved = set()
        involved.add(story["owner"]["id"])
        for c in story["contributors"]:
            involved.add(c["id"])

        # Add peripheral involvement (management, QA, arch reviews)
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
                "scenario_id": scenario["id"],
                "scenario_name": scenario["name"],
                "person_id": pid,
                "person_name": person["name"],
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

    return records, stories


# ---------------------------------------------------------------------------
# Statistical analysis
# ---------------------------------------------------------------------------

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
        deviations = [r["deviation"] for r in recs]
        corrections = [r["confirmed_cw"] - r["auto_cw"] for r in recs]
        corrected_count = sum(1 for r in recs if r["was_corrected"])

        n = len(recs)
        avg_auto = statistics.mean(auto_vals)
        avg_confirmed = statistics.mean(confirmed_vals)
        avg_correction = statistics.mean(corrections)
        correction_rate = corrected_count / n * 100 if n > 0 else 0

        std_confirmed = statistics.stdev(confirmed_vals) if n > 1 else 0.0
        min_confirmed = min(confirmed_vals)
        max_confirmed = max(confirmed_vals)

        role_stats[(role, ev_role)] = {
            "n": n,
            "avg_auto": round(avg_auto, 4),
            "avg_confirmed": round(avg_confirmed, 4),
            "avg_correction": round(avg_correction, 4),
            "correction_rate": round(correction_rate, 1),
            "std_confirmed": round(std_confirmed, 4),
            "min_confirmed": round(min_confirmed, 2),
            "max_confirmed": round(max_confirmed, 2),
            "mad": round(statistics.mean(deviations), 4) if deviations else 0.0,
        }

    return role_stats


def recommend_weight(avg_confirmed, current_cw):
    """
    Derive a recommended CW, rounded to nearest 0.05 for simplicity.
    The recommendation leans slightly toward the current value to avoid
    over-fitting to simulation noise (shrinkage factor 0.8).
    """
    shrinkage = 0.80
    raw = shrinkage * avg_confirmed + (1 - shrinkage) * current_cw
    # Round to nearest 0.05
    return round(round(raw / 0.05) * 0.05, 2)


def compute_mad(records, weights_map):
    """Compute MAD using a given set of role_weights."""
    if not records:
        return 0.0
    total_dev = 0.0
    for rec in records:
        auto_cw = weights_map.get(rec["evidence_role"], 0.15)
        confirmed = rec["confirmed_cw"]
        total_dev += abs(auto_cw - confirmed)
    return total_dev / len(records)


# ---------------------------------------------------------------------------
# Report output
# ---------------------------------------------------------------------------

def print_report(all_records, scenario_results, role_stats, recommendations,
                 mad_before, mad_after, scenario_mads):
    """Print the full calibration report."""
    total_items = sum(len(sr["stories"]) for sr in scenario_results)
    total_pairs = len(all_records)

    print()
    print("=" * 80)
    print("  EDPA CW ROLE CALIBRATION REPORT")
    print("  Metodika: EDPA v2.2 -- Multi-scenario statistická kalibrace")
    print(f"  Datum: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)
    print()
    print(f"  Celkový rozsah analýzy:")
    print(f"    Scénářů:                    {len(SCENARIOS)}")
    print(f"    Work items (stories):       {total_items}")
    print(f"    Osoba-item párů:            {total_pairs}")
    print(f"    Unikátních rolí:            {len(set(r['person_role'] for r in all_records))}")
    print()

    # Per-scenario summary
    print("-" * 80)
    print("  PER-SCENARIO SUMMARY")
    print("-" * 80)
    print(f"  {'#':<3} {'Scénář':<18} {'Tým':>4} {'Stories':>8} {'Párů':>6} "
          f"{'MAD(orig)':>10} {'MAD(calib)':>11} {'Zlepšení':>10}")
    print(f"  {'-'*76}")

    for sr in scenario_results:
        sid = sr["scenario_id"]
        sname = sr["scenario_name"]
        team_size = sr["team_size"]
        n_stories = len(sr["stories"])
        n_pairs = sr["n_records"]
        mad_o = scenario_mads[sid]["before"]
        mad_c = scenario_mads[sid]["after"]
        improvement = ((mad_o - mad_c) / mad_o * 100) if mad_o > 0 else 0
        print(f"  {sid:<3} {sname:<18} {team_size:>4} {n_stories:>8} {n_pairs:>6} "
              f"{mad_o:>10.4f} {mad_c:>11.4f} {improvement:>9.1f}%")

    print()

    # Main calibration table
    print("-" * 80)
    print("  CURRENT DEFAULTS vs RECOMMENDED")
    print("-" * 80)
    print(f"  {'Role':<12} {'Evidence':<11} {'Current':>8} {'Avg Conf':>9} "
          f"{'Recomm':>8} {'Change':>8} {'N':>5} {'Corr%':>6} "
          f"{'StdDev':>7} {'Min':>5} {'Max':>5}")
    print(f"  {'-'*96}")

    for (role, ev_role), rec_val in sorted(recommendations.items()):
        stats = role_stats[(role, ev_role)]
        current = CURRENT_ROLE_WEIGHTS.get(ev_role, 0.15)
        change = rec_val - current
        change_str = f"{change:+.2f}" if abs(change) > 0.001 else " 0.00"

        print(f"  {role:<12} {ev_role:<11} {current:>8.2f} {stats['avg_confirmed']:>9.4f} "
              f"{rec_val:>8.2f} {change_str:>8} {stats['n']:>5} "
              f"{stats['correction_rate']:>5.1f}% "
              f"{stats['std_confirmed']:>7.4f} {stats['min_confirmed']:>5.2f} "
              f"{stats['max_confirmed']:>5.2f}")

    print()

    # MAD summary
    print("-" * 80)
    print("  MAD SUMMARY (Mean Absolute Deviation)")
    print("-" * 80)
    mad_reduction = ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0
    print(f"  MAD s původními heuristikami:       {mad_before:.6f}")
    print(f"  MAD s kalibrovanými heuristikami:   {mad_after:.6f}")
    print(f"  Redukce MAD:                        {mad_reduction:.1f}%")
    print()
    print(f"  MAD reduction across scenarios: {mad_reduction:.1f}%")
    print()

    # Role-level recommendations narrative
    print("-" * 80)
    print("  ROLE-LEVEL INSIGHTS")
    print("-" * 80)

    role_narratives = {
        "BO": "Business Owner je systematicky podhodnocen. Strategická rozhodnutí, "
              "stakeholder alignment a business validace nejsou viditelné v Git signálech.",
        "PM": "Product Manager/Owner přispívá požadavky, akceptační kritéria a "
              "stakeholder management, které Git nezachytí.",
        "Arch": "Architekt provádí design reviews, mentoring a architektonická rozhodnutí "
                "napříč stories, ale Git vidí jen code review.",
        "Dev": "Developer vývojáři jsou nejlépe kalibrovaní -- Git signály "
               "přesně odrážejí jejich příspěvek.",
        "DevSecOps": "DevSecOps infrastrukturní práce (CI/CD, security scans, "
                     "deployment pipelines) je často neviditelná pro Git heuristiky.",
        "QA": "QA testerům bývá přiřazena nadhodnocená CW, protože mnoho test "
              "commitů neznamená vlastnictví feature.",
    }

    for role in ["BO", "PM", "Arch", "Dev", "DevSecOps", "QA"]:
        if role in role_narratives:
            print(f"\n  {role}:")
            # Wrap narrative to 74 chars with indent
            narrative = role_narratives[role]
            words = narrative.split()
            line = "    "
            for word in words:
                if len(line) + len(word) + 1 > 76:
                    print(line)
                    line = "    " + word
                else:
                    line += (" " if len(line) > 4 else "") + word
            if line.strip():
                print(line)

    print()
    print("=" * 80)
    print("  Kalibrace dokončena. Výstupní soubory:")
    print(f"    config/cw_heuristics_calibrated.yaml")
    print(f"    data/calibration_report.json")
    print("=" * 80)
    print()


# ---------------------------------------------------------------------------
# YAML/JSON output
# ---------------------------------------------------------------------------

def save_calibrated_heuristics(recommendations, mad_before, mad_after):
    """Save calibrated heuristics YAML."""
    # Build the new role_weights from recommendations
    # For each evidence_role, pick the weight that best represents the
    # cross-role average (weighted by record count).
    # But the EDPA engine uses evidence_role as the key, not (team_role, evidence_role).
    # So we need to derive a single weight per evidence_role.

    # Collect all recommendations per evidence_role
    ev_role_values = {}
    for (role, ev_role), weight in recommendations.items():
        ev_role_values.setdefault(ev_role, []).append((role, weight))

    # The calibrated weights: use the per-role overrides
    calibrated_role_weights = dict(CURRENT_ROLE_WEIGHTS)

    # For each evidence_role, compute a weighted average across team roles
    # This is the "single default" approach -- teams without per-role config
    # get this single number. The per-role breakdown is in role_overrides.
    for ev_role, entries in ev_role_values.items():
        if len(entries) == 1:
            calibrated_role_weights[ev_role] = entries[0][1]
        else:
            # Average across all role-specific recommendations
            avg = statistics.mean(w for _, w in entries)
            calibrated_role_weights[ev_role] = round(round(avg / 0.05) * 0.05, 2)

    # Build role_overrides section
    role_overrides = {}
    for (team_role, ev_role), weight in sorted(recommendations.items()):
        role_overrides.setdefault(team_role, {})[ev_role] = weight

    output = {
        "role_weights": calibrated_role_weights,
        "role_overrides": role_overrides,
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
            "method": "multi-scenario statistical calibration",
            "scenarios": len(SCENARIOS),
            "records_analyzed": "see calibration_report.json",
            "mad_before": round(mad_before, 6),
            "mad_after": round(mad_after, 6),
            "mad_reduction_pct": round(
                ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0, 1
            ),
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    path = CONFIG_DIR / "cw_heuristics_calibrated.yaml"
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        # Write with a header comment
        f.write("# EDPA CW Heuristics -- Calibrated via multi-scenario analysis\n")
        f.write(f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
        f.write(f"# MAD reduction: {output['calibration']['mad_reduction_pct']}%\n")
        f.write("#\n")
        f.write("# role_weights: single-value defaults (backward compatible)\n")
        f.write("# role_overrides: per-team-role fine-tuned weights\n")
        f.write("\n")
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    return path


def save_calibration_report(all_records, scenario_results, role_stats,
                            recommendations, mad_before, mad_after, scenario_mads):
    """Save detailed calibration report as JSON."""
    report = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "methodology": "EDPA v2.2 multi-scenario CW calibration",
            "scenarios_count": len(SCENARIOS),
            "total_work_items": sum(len(sr["stories"]) for sr in scenario_results),
            "total_person_item_pairs": len(all_records),
        },
        "mad_summary": {
            "before": round(mad_before, 6),
            "after": round(mad_after, 6),
            "reduction_pct": round(
                ((mad_before - mad_after) / mad_before * 100) if mad_before > 0 else 0, 1
            ),
        },
        "per_scenario": [
            {
                "id": sr["scenario_id"],
                "name": sr["scenario_name"],
                "team_size": sr["team_size"],
                "stories": len(sr["stories"]),
                "records": sr["n_records"],
                "mad_before": round(scenario_mads[sr["scenario_id"]]["before"], 6),
                "mad_after": round(scenario_mads[sr["scenario_id"]]["after"], 6),
            }
            for sr in scenario_results
        ],
        "role_statistics": {
            f"{role}|{ev_role}": {
                **stats,
                "recommended_cw": recommendations.get((role, ev_role), stats["avg_auto"]),
            }
            for (role, ev_role), stats in role_stats.items()
        },
        "recommendations": {
            f"{role}|{ev_role}": weight
            for (role, ev_role), weight in sorted(recommendations.items())
        },
        "current_defaults": CURRENT_ROLE_WEIGHTS,
        "records": all_records,
    }

    path = DATA_DIR / "calibration_report.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n  EDPA Multi-Scenario CW Calibration")
    print("  " + "=" * 45)
    print(f"  Spouštím {len(SCENARIOS)} scénářů...\n")

    all_records = []
    scenario_results = []

    for scenario in SCENARIOS:
        records, stories = simulate_scenario(scenario)
        n_records = len(records)
        all_records.extend(records)

        scenario_results.append({
            "scenario_id": scenario["id"],
            "scenario_name": scenario["name"],
            "team_size": len(scenario["team"]),
            "stories": stories,
            "n_records": n_records,
        })

        print(f"  [{scenario['id']}] {scenario['name']:<18} "
              f"tým={len(scenario['team']):>2}  stories=20  "
              f"párů={n_records:>3}")

    print(f"\n  Celkem párů: {len(all_records)}")

    # Compute role statistics
    role_stats = compute_role_statistics(all_records)

    # Derive recommendations
    recommendations = {}
    for (role, ev_role), stats in role_stats.items():
        current = CURRENT_ROLE_WEIGHTS.get(ev_role, 0.15)
        rec = recommend_weight(stats["avg_confirmed"], current)
        recommendations[(role, ev_role)] = rec

    # Compute MAD before/after per scenario
    calibrated_weights = {}
    for (role, ev_role), weight in recommendations.items():
        calibrated_weights.setdefault(ev_role, []).append(weight)
    # Average per evidence_role for the single-value fallback
    calibrated_single = {}
    for ev_role, weights in calibrated_weights.items():
        calibrated_single[ev_role] = round(statistics.mean(weights), 2)

    # Build per-role lookup for MAD
    role_ev_lookup = {}
    for (role, ev_role), weight in recommendations.items():
        role_ev_lookup[(role, ev_role)] = weight

    def mad_with_weights(records, use_calibrated=False):
        """Compute MAD using either original or calibrated weights."""
        if not records:
            return 0.0
        total = 0.0
        for r in records:
            if use_calibrated:
                # Use role-specific calibrated weight
                w = role_ev_lookup.get(
                    (r["person_role"], r["evidence_role"]),
                    calibrated_single.get(r["evidence_role"], 0.15)
                )
            else:
                w = CURRENT_ROLE_WEIGHTS.get(r["evidence_role"], 0.15)
            total += abs(w - r["confirmed_cw"])
        return total / len(records)

    mad_before = mad_with_weights(all_records, use_calibrated=False)
    mad_after = mad_with_weights(all_records, use_calibrated=True)

    # Per-scenario MADs
    scenario_mads = {}
    for sr in scenario_results:
        sid = sr["scenario_id"]
        sc_records = [r for r in all_records if r["scenario_id"] == sid]
        scenario_mads[sid] = {
            "before": mad_with_weights(sc_records, use_calibrated=False),
            "after": mad_with_weights(sc_records, use_calibrated=True),
        }

    # Print report
    print_report(all_records, scenario_results, role_stats, recommendations,
                 mad_before, mad_after, scenario_mads)

    # Save outputs
    yaml_path = save_calibrated_heuristics(recommendations, mad_before, mad_after)
    json_path = save_calibration_report(all_records, scenario_results, role_stats,
                                        recommendations, mad_before, mad_after,
                                        scenario_mads)

    print(f"  Soubory uloženy:")
    print(f"    {yaml_path.relative_to(ROOT)}")
    print(f"    {json_path.relative_to(ROOT)}")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
