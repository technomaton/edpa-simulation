#!/usr/bin/env python3
"""
EDPA Full Simulation -- Medical Platform (kashealth.cz)

Simulates 2 Planning Intervals of EDPA-managed delivery with realistic Git history,
runs the EDPA engine per iteration, and produces audit-ready reports and snapshots.

Models realistic estimation: ~80% planning capacity with delivery variance across
iterations (under-delivery, good delivery, over-delivery scenarios).

Usage:
    python scripts/simulate.py --dry-run          # Print plan without executing
    python scripts/simulate.py --pi 1             # Simulate PI-1 only
    python scripts/simulate.py --pi 2             # Simulate PI-2 only
    python scripts/simulate.py --pi all           # Simulate both PIs
"""

import argparse
import copy
import hashlib
import json
import os
import random
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML required. Install with: pip install pyyaml")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
REPORTS_DIR = ROOT / "reports"
SNAPSHOTS_DIR = ROOT / "snapshots"
DATA_DIR = ROOT / "data"

PI1_START = datetime(2026, 4, 1, 8, 0, 0, tzinfo=timezone.utc)
ITERATION_DAYS = 14  # 2-week cadence

RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# Planning & delivery variance
# ---------------------------------------------------------------------------

PLANNING_FACTOR = 0.80  # Plan to ~80% of capacity (story points)

# Delivery scenarios: (factor, label, description)
# factor = fraction of planned SP actually delivered
DELIVERY_SCENARIOS = [
    (0.65, "UNDER", "Blockers + sick days, 2 stories postponed"),
    (0.75, "UNDER", "Dependencies delayed, 1 story not started"),
    (0.85, "GOOD",  "Slight overestimation, buffer absorbed"),
    (0.90, "GOOD",  "Well planned, minor adjustments"),
    (0.95, "GOOD",  "Nearly perfect sprint"),
    (1.00, "EXACT", "All planned work delivered"),
    (1.10, "OVER",  "Team picked up 1 unplanned story"),
    (1.20, "OVER",  "Fast iteration, 2 extra stories completed"),
]

# Fixed scenario assignment per iteration (deterministic, models team learning)
# Key: (pi_num, iter_num) -> index into DELIVERY_SCENARIOS
ITERATION_SCENARIO_MAP = {
    # PI-1: team is learning
    (1, 1): 0,  # 0.65 UNDER -- first iteration, team overestimates
    (1, 2): 2,  # 0.85 GOOD  -- adjusting
    (1, 3): 4,  # 0.95 GOOD  -- improving
    (1, 4): 6,  # 1.10 OVER  -- confident, picks up extra
    # PI-2: team improved
    (2, 1): 3,  # 0.90 GOOD  -- strong start
    (2, 2): 1,  # 0.75 UNDER -- unexpected blocker
    (2, 3): 4,  # 0.95 GOOD  -- back on track
    (2, 4): 5,  # 1.00 EXACT -- well-calibrated
}

# ---------------------------------------------------------------------------
# Team definition (loaded from config but also inlined for commit authoring)
# ---------------------------------------------------------------------------

TEAM = [
    {
        "id": "novak", "name": "Jan Novak", "role": "BO",
        "fte": 0.30, "capacity": 24, "email": "novak@kashealth.cz",
        "team": "Management",
    },
    {
        "id": "kralova", "name": "Marie Kralova", "role": "PM",
        "fte": 0.50, "capacity": 40, "email": "kralova@kashealth.cz",
        "team": "Management",
    },
    {
        "id": "urbanek", "name": "Jaroslav Urbanek", "role": "Arch",
        "fte": 0.70, "capacity": 56, "email": "urbanek@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "svoboda", "name": "Petr Svoboda", "role": "Dev",
        "fte": 1.00, "capacity": 80, "email": "svoboda@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "cerny", "name": "Tomas Cerny", "role": "Dev",
        "fte": 1.00, "capacity": 80, "email": "cerny@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "tuma", "name": "Ondrej Tuma", "role": "DevSecOps",
        "fte": 0.80, "capacity": 64, "email": "tuma@kashealth.cz",
        "team": "Platform",
    },
    {
        "id": "nemcova", "name": "Katerina Nemcova", "role": "QA",
        "fte": 1.20, "capacity": 96, "email": "nemcova@kashealth.cz",
        "team": "Platform",
    },
]

TEAM_BY_ID = {m["id"]: m for m in TEAM}
TEAM_CAPACITY = sum(m["capacity"] for m in TEAM)  # 440h

# ---------------------------------------------------------------------------
# Work Item Hierarchy -- PI-1
# ---------------------------------------------------------------------------

PI1_EPICS = [
    {
        "id": "E-10", "title": "Anonymizacni modul", "js": 13,
        "features": [
            {
                "id": "F-100", "title": "K-anonymity engine", "js": 8,
                "stories": [
                    {"id": "S-1001", "title": "Implementace QI detekce", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek", "nemcova"]},
                    {"id": "S-1002", "title": "Generalizacni hierarchie", "js": 3,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-1003", "title": "K-anonymity validator", "js": 5,
                     "owner": "cerny", "contributors": ["nemcova"]},
                    {"id": "S-1004", "title": "Unit testy anonymizace", "js": 2,
                     "owner": "nemcova", "contributors": ["svoboda"]},
                ],
            },
            {
                "id": "F-101", "title": "L-diversity rozsireni", "js": 5,
                "stories": [
                    {"id": "S-1005", "title": "L-diversity algoritmus", "js": 5,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-1006", "title": "Konfigurace citlivych atributu", "js": 3,
                     "owner": "svoboda", "contributors": ["tuma"]},
                    {"id": "S-1007", "title": "Benchmark vykonu anonymizace", "js": 2,
                     "owner": "nemcova", "contributors": ["cerny"]},
                ],
            },
            {
                "id": "F-102", "title": "Anonymizacni pipeline", "js": 8,
                "stories": [
                    {"id": "S-1008", "title": "Celery task pro anonymizaci", "js": 5,
                     "owner": "tuma", "contributors": ["svoboda", "cerny"]},
                    {"id": "S-1009", "title": "Monitoring pipeline metriky", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                    {"id": "S-1010", "title": "Error handling a retry logika", "js": 3,
                     "owner": "cerny", "contributors": ["tuma"]},
                    {"id": "S-1011", "title": "Integracni testy pipeline", "js": 3,
                     "owner": "nemcova", "contributors": ["tuma", "svoboda"]},
                ],
            },
        ],
    },
    {
        "id": "E-11", "title": "Datovy e-shop API", "js": 8,
        "features": [
            {
                "id": "F-110", "title": "Katalog datasetu", "js": 5,
                "stories": [
                    {"id": "S-1101", "title": "REST endpoint /datasets", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek"]},
                    {"id": "S-1102", "title": "Filtrovani a strankovani", "js": 3,
                     "owner": "cerny", "contributors": ["svoboda"]},
                    {"id": "S-1103", "title": "OpenAPI specifikace katalogu", "js": 2,
                     "owner": "urbanek", "contributors": ["kralova"]},
                ],
            },
            {
                "id": "F-111", "title": "Objednavkovy proces", "js": 5,
                "stories": [
                    {"id": "S-1104", "title": "Nakupni kosik API", "js": 5,
                     "owner": "cerny", "contributors": ["svoboda"]},
                    {"id": "S-1105", "title": "Platebni integrace mock", "js": 3,
                     "owner": "svoboda", "contributors": ["tuma"]},
                    {"id": "S-1106", "title": "E2E testy objednavky", "js": 3,
                     "owner": "nemcova", "contributors": ["cerny", "svoboda"]},
                ],
            },
            {
                "id": "F-112", "title": "Autorizace a pristupova prava", "js": 5,
                "stories": [
                    {"id": "S-1107", "title": "RBAC model pro e-shop", "js": 5,
                     "owner": "tuma", "contributors": ["urbanek"]},
                    {"id": "S-1108", "title": "JWT token management", "js": 3,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-1109", "title": "Audit log pristupu", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                ],
            },
        ],
    },
    {
        "id": "E-12", "title": "OMOP CDM integrace", "js": 5,
        "features": [
            {
                "id": "F-120", "title": "OMOP schema migrace", "js": 5,
                "stories": [
                    {"id": "S-1201", "title": "Alembic migrace OMOP tabulek", "js": 5,
                     "owner": "urbanek", "contributors": ["svoboda"]},
                    {"id": "S-1202", "title": "ETL transformace zdrojovych dat", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek", "cerny"]},
                    {"id": "S-1203", "title": "Validace OMOP integrity", "js": 3,
                     "owner": "nemcova", "contributors": ["urbanek"]},
                ],
            },
            {
                "id": "F-121", "title": "OMOP vocabular sluzba", "js": 3,
                "stories": [
                    {"id": "S-1204", "title": "Vocabulary lookup API", "js": 3,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-1205", "title": "Concept mapping nastroj", "js": 3,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-1206", "title": "Cache vrstva pro vocabulary", "js": 2,
                     "owner": "tuma", "contributors": ["cerny"]},
                ],
            },
        ],
    },
]

# ---------------------------------------------------------------------------
# Work Item Hierarchy -- PI-2
# ---------------------------------------------------------------------------

PI2_EPICS = [
    {
        "id": "E-20", "title": "Pokrocila analytika", "js": 8,
        "features": [
            {
                "id": "F-200", "title": "Statisticky engine", "js": 5,
                "stories": [
                    {"id": "S-2001", "title": "Deskriptivni statistiky modul", "js": 5,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-2002", "title": "Korelacni analyza sluzba", "js": 5,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-2003", "title": "Regresni model API", "js": 5,
                     "owner": "cerny", "contributors": ["svoboda", "urbanek"]},
                    {"id": "S-2004", "title": "Statisticke testy modul", "js": 3,
                     "owner": "nemcova", "contributors": ["cerny"]},
                ],
            },
            {
                "id": "F-201", "title": "Vizualizacni sluzba", "js": 5,
                "stories": [
                    {"id": "S-2005", "title": "Chart rendering engine", "js": 5,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-2006", "title": "Dashboard layout API", "js": 3,
                     "owner": "cerny", "contributors": ["kralova"]},
                    {"id": "S-2007", "title": "Export do PDF/PNG", "js": 3,
                     "owner": "svoboda", "contributors": ["nemcova"]},
                ],
            },
            {
                "id": "F-202", "title": "Analyticky sandbox", "js": 5,
                "stories": [
                    {"id": "S-2008", "title": "Jupyter notebook integrace", "js": 5,
                     "owner": "tuma", "contributors": ["urbanek", "svoboda"]},
                    {"id": "S-2009", "title": "Sandbox izolace a limity", "js": 3,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-2010", "title": "Sdileni notebooku API", "js": 3,
                     "owner": "cerny", "contributors": ["tuma"]},
                ],
            },
        ],
    },
    {
        "id": "E-21", "title": "Compliance a GDPR modul", "js": 8,
        "features": [
            {
                "id": "F-210", "title": "Consent management", "js": 5,
                "stories": [
                    {"id": "S-2101", "title": "Consent storage model", "js": 5,
                     "owner": "urbanek", "contributors": ["tuma"]},
                    {"id": "S-2102", "title": "Consent collection UI API", "js": 3,
                     "owner": "svoboda", "contributors": ["kralova"]},
                    {"id": "S-2103", "title": "Consent audit trail", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                    {"id": "S-2104", "title": "Consent withdrawal workflow", "js": 3,
                     "owner": "cerny", "contributors": ["tuma", "nemcova"]},
                ],
            },
            {
                "id": "F-211", "title": "Data retention engine", "js": 5,
                "stories": [
                    {"id": "S-2105", "title": "Retention policy model", "js": 5,
                     "owner": "urbanek", "contributors": ["tuma"]},
                    {"id": "S-2106", "title": "Automaticka expiracni sluzba", "js": 5,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-2107", "title": "Retention reporting dashboard", "js": 3,
                     "owner": "svoboda", "contributors": ["nemcova"]},
                ],
            },
        ],
    },
    {
        "id": "E-22", "title": "Platformova stabilizace", "js": 5,
        "features": [
            {
                "id": "F-220", "title": "Observabilita", "js": 5,
                "stories": [
                    {"id": "S-2201", "title": "OpenTelemetry integrace", "js": 5,
                     "owner": "tuma", "contributors": ["urbanek"]},
                    {"id": "S-2202", "title": "Grafana dashboardy", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                    {"id": "S-2203", "title": "Alerting pravidla", "js": 3,
                     "owner": "tuma", "contributors": ["cerny"]},
                ],
            },
            {
                "id": "F-221", "title": "Performance optimalizace", "js": 3,
                "stories": [
                    {"id": "S-2204", "title": "Database query optimalizace", "js": 5,
                     "owner": "cerny", "contributors": ["urbanek", "svoboda"]},
                    {"id": "S-2205", "title": "Redis caching vrstva", "js": 3,
                     "owner": "svoboda", "contributors": ["tuma"]},
                    {"id": "S-2206", "title": "Load testing sada", "js": 3,
                     "owner": "nemcova", "contributors": ["tuma", "cerny"]},
                    {"id": "S-2207", "title": "API response time benchmark", "js": 2,
                     "owner": "nemcova", "contributors": ["svoboda"]},
                ],
            },
        ],
    },
]

# ---------------------------------------------------------------------------
# Iteration planning -- which stories go into which iteration
# ---------------------------------------------------------------------------

# PI-1: 4 delivery iterations + 1 IP
PI1_ITERATION_STORIES = {
    1: ["S-1001", "S-1002", "S-1101", "S-1102", "S-1201", "S-1103", "S-1107"],
    2: ["S-1003", "S-1004", "S-1005", "S-1104", "S-1105", "S-1202", "S-1108"],
    3: ["S-1006", "S-1007", "S-1008", "S-1106", "S-1203", "S-1204", "S-1109"],
    4: ["S-1009", "S-1010", "S-1011", "S-1205", "S-1206"],
    5: [],  # IP iteration -- no new stories, hardening and planning
}

# PI-2: 4 delivery iterations + 1 IP
PI2_ITERATION_STORIES = {
    1: ["S-2001", "S-2002", "S-2101", "S-2102", "S-2201", "S-2105", "S-2204"],
    2: ["S-2003", "S-2004", "S-2005", "S-2103", "S-2106", "S-2202", "S-2205"],
    3: ["S-2006", "S-2007", "S-2008", "S-2104", "S-2107", "S-2203", "S-2206"],
    4: ["S-2009", "S-2010", "S-2009", "S-2207"],  # lighter iteration before IP
    5: [],  # IP iteration
}

# ---------------------------------------------------------------------------
# Unplanned story templates (used when delivery_factor > 1.0)
# ---------------------------------------------------------------------------

# Counter for generating unique unplanned story IDs
_unplanned_counter = {"value": 9000}


def make_unplanned_story(pi_num, iter_num, rng, all_stories):
    """Generate an unplanned small story (JS 1-3) for over-delivery scenarios."""
    _unplanned_counter["value"] += 1
    sid = f"S-{_unplanned_counter['value']}"

    templates = [
        ("Hotfix: oprava validace vstupu", "svoboda"),
        ("Refactor: cleanup utility modulu", "cerny"),
        ("Chore: aktualizace CI pipeline", "tuma"),
        ("Fix: oprava race condition v cache", "svoboda"),
        ("Chore: migrace konfiguracnich souboru", "tuma"),
        ("Fix: timeout handling v API klientu", "cerny"),
        ("Refactor: typove anotace modulu", "svoboda"),
        ("Chore: upgrade zavislosti", "tuma"),
    ]
    title, owner = rng.choice(templates)
    js = rng.choice([1, 2, 2, 3])

    # Pick a contributor from the team (not the owner)
    possible = [m["id"] for m in TEAM if m["id"] != owner and m["role"] in ("Dev", "DevSecOps", "QA")]
    contributor = rng.choice(possible) if possible else "nemcova"

    # Attach to a plausible epic/feature from the PI
    epics = PI1_EPICS if pi_num == 1 else PI2_EPICS
    epic = rng.choice(epics)
    feature = rng.choice(epic["features"])

    story = {
        "id": sid,
        "title": title,
        "js": js,
        "owner": owner,
        "contributors": [contributor],
        "unplanned": True,
        "epic_id": epic["id"],
        "epic_title": epic["title"],
        "feature_id": feature["id"],
        "feature_title": feature["title"],
        "feature_js": feature["js"],
        "epic_js": epic["js"],
    }
    return story


# ---------------------------------------------------------------------------
# Code generation templates (realistic file content per role/type)
# ---------------------------------------------------------------------------

CODE_TEMPLATES = {
    "python_module": '''\
"""
{module_doc}

Part of {epic_title} -- {feature_title}
Story: {story_id} -- {story_title}
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class {class_name}:
    """
    {class_doc}
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {{}}
        self._initialized = False
        logger.info("Initializing {class_name}")

    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process input data according to {story_id} requirements."""
        if not self._initialized:
            self._setup()
        results = []
        for record in data:
            transformed = self._transform(record)
            if self._validate(transformed):
                results.append(transformed)
        logger.info("Processed %d records, %d valid", len(data), len(results))
        return results

    def _setup(self):
        """Initialize internal state."""
        self._initialized = True

    def _transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transformation rules."""
        return {{**record, "processed": True}}

    def _validate(self, record: Dict[str, Any]) -> bool:
        """Validate transformed record."""
        return record.get("processed", False)
''',

    "test_module": '''\
"""
Tests for {story_id}: {story_title}
"""

import pytest
from unittest.mock import MagicMock, patch


class Test{class_name}:
    """Test suite for {class_name}."""

    @pytest.fixture
    def instance(self):
        """Create test instance."""
        return MagicMock()

    def test_basic_processing(self, instance):
        """Verify basic data processing."""
        input_data = [{{"id": 1, "value": "test"}}]
        result = instance.process(input_data)
        assert result is not None

    def test_empty_input(self, instance):
        """Verify handling of empty input."""
        result = instance.process([])
        assert result is not None

    def test_invalid_data_handling(self, instance):
        """Verify graceful handling of invalid data."""
        invalid_data = [{{"corrupt": True}}]
        result = instance.process(invalid_data)
        assert result is not None

    def test_configuration(self):
        """Verify configuration is applied correctly."""
        config = {{"threshold": 0.5, "mode": "strict"}}
        # Configuration test for {story_id}
        assert config["threshold"] == 0.5
''',

    "config_file": '''\
# Configuration for {story_id}: {story_title}
# Part of {feature_title}

service:
  name: "{service_name}"
  version: "1.0.0"

settings:
  enabled: true
  log_level: INFO
  max_retries: 3
  timeout_seconds: 30

database:
  pool_size: 5
  max_overflow: 10
''',

    "ci_config": '''\
# CI/CD configuration for {story_id}
# Managed by DevSecOps

name: {service_name}
on:
  push:
    branches: [main, "feature/**"]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run tests
        run: pytest tests/ -v --cov
      - name: Security scan
        run: bandit -r src/ -ll
''',

    "api_spec": '''\
# API Specification for {story_id}: {story_title}
# Part of {feature_title}

openapi: "3.0.3"
info:
  title: "{feature_title}"
  version: "1.0.0"
paths:
  /{endpoint}:
    get:
      summary: "List {endpoint}"
      responses:
        "200":
          description: "Successful response"
    post:
      summary: "Create {endpoint} item"
      responses:
        "201":
          description: "Created"
''',
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def slugify(text):
    """Convert text to URL-friendly slug."""
    import unicodedata
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = text.replace(" ", "-")
    return "".join(c for c in text if c.isalnum() or c == "-")


def make_class_name(story_title):
    """Convert story title to PascalCase class name."""
    import unicodedata
    text = unicodedata.normalize("NFKD", story_title)
    text = text.encode("ascii", "ignore").decode("ascii")
    words = text.split()
    return "".join(w.capitalize() for w in words if w.isalpha())[:40]


def flatten_stories(epics):
    """Extract all stories from epic hierarchy into a flat dict keyed by story ID."""
    stories = {}
    for epic in epics:
        for feature in epic["features"]:
            for story in feature["stories"]:
                stories[story["id"]] = {
                    **story,
                    "epic_id": epic["id"],
                    "epic_title": epic["title"],
                    "feature_id": feature["id"],
                    "feature_title": feature["title"],
                    "feature_js": feature["js"],
                    "epic_js": epic["js"],
                }
    return stories


def iteration_date_range(pi_start, iteration_num):
    """Return (start, end) datetimes for an iteration."""
    start = pi_start + timedelta(days=(iteration_num - 1) * ITERATION_DAYS)
    end = start + timedelta(days=ITERATION_DAYS - 1)
    return start, end


def random_time_in_range(start_dt, end_dt, rng):
    """Pick a random datetime between start and end."""
    delta = (end_dt - start_dt).total_seconds()
    offset = rng.uniform(0, delta)
    return start_dt + timedelta(seconds=offset)


def get_scenario(pi_num, iter_num):
    """Get the delivery scenario for a given PI and iteration."""
    idx = ITERATION_SCENARIO_MAP.get((pi_num, iter_num))
    if idx is not None:
        return DELIVERY_SCENARIOS[idx]
    # Fallback: good delivery
    return (0.90, "GOOD", "Well planned, minor adjustments")


def select_spillover_stories(story_ids, all_stories, target_sp_to_spill, rng):
    """
    Select stories to mark as spillover so that approximately target_sp_to_spill
    SP are removed. Prefers removing stories from the end of the list (later planned)
    and smaller stories first to be realistic.

    Returns (delivered_ids, spillover_ids).
    """
    if target_sp_to_spill <= 0:
        return list(story_ids), []

    # Build list with JS, prefer spilling from end
    candidates = []
    for sid in story_ids:
        story = all_stories.get(sid)
        if story:
            candidates.append((sid, story["js"]))

    # Sort by position (reverse -- later stories spill first)
    # then by JS ascending (smaller stories spill first for partial)
    candidates_indexed = list(enumerate(candidates))
    candidates_indexed.sort(key=lambda x: (-x[0], x[1][1]))

    spilled_sp = 0
    spillover_ids = []
    for _orig_idx, (sid, js) in candidates_indexed:
        if spilled_sp >= target_sp_to_spill:
            break
        spillover_ids.append(sid)
        spilled_sp += js

    delivered_ids = [sid for sid in story_ids if sid not in spillover_ids]
    return delivered_ids, spillover_ids


def determine_commit_plan(story, rng):
    """
    Determine who commits what for a given story.
    Returns list of (person_id, file_type, commit_msg_prefix) tuples.
    """
    owner = story["owner"]
    contributors = story.get("contributors", [])
    owner_member = TEAM_BY_ID[owner]
    plan = []

    # Owner makes most commits (3-8)
    owner_role = owner_member["role"]
    if owner_role in ("Dev",):
        n_owner = rng.randint(5, 8)
    elif owner_role == "Arch":
        n_owner = rng.randint(3, 5)
    elif owner_role == "DevSecOps":
        n_owner = rng.randint(4, 6)
    elif owner_role == "QA":
        n_owner = rng.randint(4, 7)
    else:
        n_owner = rng.randint(2, 4)

    file_types_by_role = {
        "Dev": ["python_module", "python_module", "config_file"],
        "Arch": ["python_module", "api_spec"],
        "DevSecOps": ["ci_config", "config_file", "python_module"],
        "QA": ["test_module", "test_module", "config_file"],
        "PM": ["api_spec"],
        "BO": ["api_spec"],
    }

    owner_types = file_types_by_role.get(owner_role, ["python_module"])
    for i in range(n_owner):
        ft = rng.choice(owner_types)
        prefix_choices = ["feat", "refactor", "fix", "chore"]
        weights = [0.5, 0.2, 0.2, 0.1]
        prefix = rng.choices(prefix_choices, weights=weights, k=1)[0]
        plan.append((owner, ft, prefix))

    # Contributors make fewer commits (1-3)
    for contrib_id in contributors:
        contrib_member = TEAM_BY_ID[contrib_id]
        contrib_role = contrib_member["role"]
        n_contrib = rng.randint(1, 3)
        contrib_types = file_types_by_role.get(contrib_role, ["python_module"])
        for i in range(n_contrib):
            ft = rng.choice(contrib_types)
            prefix = rng.choice(["feat", "fix", "test", "chore"])
            plan.append((contrib_id, ft, prefix))

    # Management adds occasional comments/specs (BO and PM)
    if rng.random() < 0.3:  # 30% chance BO comments
        plan.append(("novak", "api_spec", "docs"))
    if rng.random() < 0.4:  # 40% chance PM comments
        plan.append(("kralova", "api_spec", "docs"))

    # QA always reviews -- add test if not already contributor
    if "nemcova" not in [owner] + contributors:
        if rng.random() < 0.5:  # 50% chance QA adds review test
            plan.append(("nemcova", "test_module", "test"))

    # Arch reviews cross-cutting stories
    if "urbanek" not in [owner] + contributors:
        if story.get("epic_js", 0) >= 8 and rng.random() < 0.35:
            plan.append(("urbanek", "api_spec", "refactor"))

    rng.shuffle(plan)
    return plan


def generate_file_content(story, file_type, commit_index):
    """Generate realistic file content for a commit."""
    sid = story["id"]
    s_title = story["title"]
    f_title = story.get("feature_title", "Feature")
    e_title = story.get("epic_title", "Epic")
    slug = slugify(s_title)
    class_name = make_class_name(s_title) or "Component"
    service_name = slugify(f_title) or "service"
    endpoint = slugify(s_title).replace("-", "_")

    template = CODE_TEMPLATES.get(file_type, CODE_TEMPLATES["python_module"])

    content = template.format(
        module_doc=f"Module for {sid}: {s_title}",
        epic_title=e_title,
        feature_title=f_title,
        story_id=sid,
        story_title=s_title,
        class_name=class_name,
        class_doc=f"Implements {sid} -- {s_title}",
        service_name=service_name,
        endpoint=endpoint,
    )

    return content


def file_path_for(story, file_type, commit_index):
    """Determine the file path for a generated file."""
    sid = story["id"]
    slug = slugify(story["title"])
    epic_slug = slugify(story.get("epic_title", "epic"))
    feature_slug = slugify(story.get("feature_title", "feature"))

    base = f"src/{epic_slug}/{feature_slug}"

    if file_type == "python_module":
        return f"{base}/{slug}_{commit_index}.py"
    elif file_type == "test_module":
        return f"tests/{feature_slug}/test_{slug}_{commit_index}.py"
    elif file_type == "config_file":
        return f"config/services/{slug}_{commit_index}.yaml"
    elif file_type == "ci_config":
        return f".github/workflows/{slug}_{commit_index}.yml"
    elif file_type == "api_spec":
        return f"docs/api/{slug}_{commit_index}.yaml"
    else:
        return f"{base}/{slug}_{commit_index}.txt"


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------


def git(args, cwd=None, env_override=None):
    """Run a git command, return (returncode, stdout)."""
    cmd = ["git"] + args
    env = os.environ.copy()
    if env_override:
        env.update(env_override)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd or str(ROOT),
        env=env,
    )
    if result.returncode != 0 and "--no-pager" not in args:
        # Tolerate some failures (branch already exists, etc.)
        pass
    return result.returncode, result.stdout.strip()


def git_commit_as(person_id, message, date_str, cwd=None):
    """Create a git commit attributed to a specific person."""
    member = TEAM_BY_ID[person_id]
    env = {
        "GIT_AUTHOR_NAME": member["name"],
        "GIT_AUTHOR_EMAIL": member["email"],
        "GIT_AUTHOR_DATE": date_str,
        "GIT_COMMITTER_NAME": member["name"],
        "GIT_COMMITTER_EMAIL": member["email"],
        "GIT_COMMITTER_DATE": date_str,
    }
    return git(["commit", "-m", message, "--allow-empty"], cwd=cwd, env_override=env)


def ensure_main_branch():
    """Make sure we are on main and it exists."""
    rc, current = git(["branch", "--show-current"])
    if current != "main":
        git(["checkout", "-b", "main"])


# ---------------------------------------------------------------------------
# EDPA Engine integration
# ---------------------------------------------------------------------------


def build_edpa_items(stories_for_iteration, all_stories, commit_log):
    """
    Build item dicts suitable for the EDPA engine from the simulation state.

    commit_log: list of {story_id, person_id, role} dicts for the iteration
    """
    items = []
    for sid in stories_for_iteration:
        story = all_stories.get(sid)
        if not story:
            continue

        # Gather evidence from commit log
        story_commits = [c for c in commit_log if c["story_id"] == sid]
        commit_authors = list(set(c["person_id"] for c in story_commits))
        owner = story["owner"]
        contributors = story.get("contributors", [])

        # Build commenters -- PM, BO may have commented
        commenters = []
        for c in story_commits:
            if TEAM_BY_ID[c["person_id"]]["role"] in ("PM", "BO"):
                commenters.append(c["person_id"])
        commenters = list(set(commenters))

        # PR reviewer is typically the architect or QA
        pr_reviewers = []
        for pid in commit_authors:
            if TEAM_BY_ID[pid]["role"] in ("Arch", "QA") and pid != owner:
                pr_reviewers.append(pid)
        # If no reviewer, add architect
        if not pr_reviewers and "urbanek" not in commit_authors:
            pr_reviewers.append("urbanek")

        items.append({
            "id": sid,
            "level": "Story",
            "job_size": story["js"],
            "assignees": [{"login": owner}],
            "body": "",
            "pr_author": owner,
            "commit_authors": commit_authors,
            "pr_reviewers": pr_reviewers,
            "commenters": commenters,
        })

    return items


def run_edpa_engine(items, iteration_id, mode="simple"):
    """Run the EDPA engine with the given items and return results."""
    # Import the engine module
    engine_path = ROOT / "scripts" / "edpa_engine.py"
    if not engine_path.exists():
        print(f"  WARNING: edpa_engine.py not found at {engine_path}")
        return None

    # We invoke the engine functions directly
    sys.path.insert(0, str(ROOT / "scripts"))
    try:
        import importlib
        if "edpa_engine" in sys.modules:
            importlib.reload(sys.modules["edpa_engine"])
        import edpa_engine

        capacity_config = {
            "people": [
                {
                    "id": m["id"],
                    "name": m["name"],
                    "role": m["role"],
                    "team": m["team"],
                    "fte": m["fte"],
                    "capacity_per_iteration": m["capacity"],
                    "email": m["email"],
                }
                for m in TEAM
            ]
        }

        heuristics_path = CONFIG_DIR / "cw_heuristics.yaml"
        with open(heuristics_path) as f:
            heuristics = yaml.safe_load(f)

        results = edpa_engine.run_edpa(capacity_config, heuristics, items, mode=mode)
        return results

    except Exception as e:
        print(f"  WARNING: EDPA engine error: {e}")
        return None
    finally:
        if str(ROOT / "scripts") in sys.path:
            sys.path.remove(str(ROOT / "scripts"))


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def save_iteration_report(results, iteration_id, items, commit_log,
                          scenario_info=None):
    """Save JSON report and per-person vykaz for an iteration."""
    report_dir = REPORTS_DIR / f"iteration-{iteration_id}"
    report_dir.mkdir(parents=True, exist_ok=True)

    all_passed = all(r["invariant_ok"] for r in results if r["items"])
    team_total = sum(r["total_derived"] for r in results)

    report = {
        "iteration": iteration_id,
        "mode": "simple",
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "methodology": "EDPA v1.0.0",
        "people": results,
        "team_total": round(team_total, 2),
        "all_invariants_passed": all_passed,
        "items_count": len(items),
        "commit_count": len(commit_log),
    }

    # Include scenario info if provided
    if scenario_info:
        report["scenario"] = scenario_info

    # Save JSON report
    report_path = report_dir / "edpa_results.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Save per-person vykaz (markdown)
    for person_result in results:
        pid = person_result["id"]
        member = TEAM_BY_ID.get(pid, {})
        vykaz_path = report_dir / f"vykaz-{pid}.md"

        lines = [
            f"# Vykaz -- {person_result['name']}",
            f"",
            f"- **Iterace:** {iteration_id}",
            f"- **Role:** {person_result['role']}",
            f"- **FTE:** {member.get('fte', 'N/A')}",
            f"- **Kapacita:** {person_result['capacity']}h",
            f"- **Odvozene hodiny:** {person_result['total_derived']}h",
            f"- **Invarianty:** {'OK' if person_result['invariant_ok'] else 'FAIL'}",
            f"",
            f"## Polozky",
            f"",
            f"| Polozka | Uroven | JS | CW | Score | Podil | Hodiny |",
            f"|---------|--------|----|----|-------|-------|--------|",
        ]

        for item in person_result["items"]:
            lines.append(
                f"| {item['id']} | {item['level']} | {item['js']} "
                f"| {item['cw']:.2f} | {item['score']:.2f} "
                f"| {item['ratio']:.1%} | {item['hours']:.1f}h |"
            )

        lines.extend([
            f"",
            f"## Evidence",
            f"",
        ])
        for item in person_result["items"]:
            lines.append(f"- **{item['id']}**: {', '.join(item['evidence'])}")

        with open(vykaz_path, "w") as f:
            f.write("\n".join(lines) + "\n")

    return report_path


def save_snapshot(results, iteration_id, items, commit_log, scenario_info=None):
    """Save a frozen snapshot of the iteration state."""
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = SNAPSHOTS_DIR / f"iteration-{iteration_id}.json"

    snapshot = {
        "iteration": iteration_id,
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "methodology": "EDPA v1.0.0",
        "team_capacity": TEAM_CAPACITY,
        "items": items,
        "commit_count": len(commit_log),
        "results": results,
        "invariants": {
            "all_passed": all(r["invariant_ok"] for r in results if r["items"]),
            "ratio_sums": {
                r["id"]: round(sum(i["ratio"] for i in r["items"]), 6)
                for r in results if r["items"]
            },
            "capacity_match": {
                r["id"]: abs(r["total_derived"] - r["capacity"]) < 0.01
                for r in results if r["items"]
            },
        },
    }

    if scenario_info:
        snapshot["scenario"] = scenario_info

    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    return snapshot_path


# ---------------------------------------------------------------------------
# Ground truth generation for CW evaluation
# ---------------------------------------------------------------------------


def generate_ground_truth(commit_log, all_stories, pi_label):
    """Generate ground truth YAML with realistic team corrections.

    The confirmed_cw DIFFERS from auto_cw to simulate real retro feedback:
    - Arch/PM/BO systematically undervalued by auto-detection
    - QA sometimes over-detected (many test commits inflate CW)
    - Dev pair-programming under-detected
    """
    rng = random.Random(hash(pi_label) + 7)
    records = []

    correction_patterns = {
        ("Arch", "reviewer"): (0.65, "up", [0.35, 0.40, 0.50, 0.60]),
        ("Arch", "consulted"): (0.50, "up", [0.25, 0.30, 0.40]),
        ("PM", "consulted"): (0.70, "up", [0.25, 0.30, 0.40, 0.50]),
        ("PM", "reviewer"): (0.40, "up", [0.35, 0.40]),
        ("BO", "consulted"): (0.80, "up", [0.30, 0.35, 0.40, 0.50]),
        ("QA", "owner"): (0.35, "down", [0.60, 0.70, 0.80]),
        ("QA", "key"): (0.25, "down", [0.40, 0.50]),
        ("Dev", "reviewer"): (0.30, "up", [0.35, 0.40, 0.50, 0.60]),
        ("Dev", "consulted"): (0.20, "up", [0.25, 0.30]),
        ("DevSecOps", "consulted"): (0.45, "up", [0.25, 0.30, 0.40]),
        ("DevSecOps", "reviewer"): (0.35, "up", [0.35, 0.40, 0.50]),
    }

    pairs = {}
    for c in commit_log:
        key = (c["person_id"], c["story_id"])
        pairs.setdefault(key, []).append(c)

    for (pid, sid), commits in pairs.items():
        story = all_stories.get(sid)
        if not story:
            continue

        owner = story["owner"]
        contributors = story.get("contributors", [])
        member = TEAM_BY_ID.get(pid, {})
        role = member.get("role", "")

        if pid == owner:
            evidence_role = "owner"
            auto_cw = 1.0
        elif pid in contributors:
            evidence_role = "key"
            auto_cw = 0.6
        else:
            if role in ("Arch", "QA"):
                evidence_role = "reviewer"
                auto_cw = 0.25
            else:
                evidence_role = "consulted"
                auto_cw = 0.15

        confirmed_cw = auto_cw
        correction_key = (role, evidence_role)
        if correction_key in correction_patterns:
            prob, direction, values = correction_patterns[correction_key]
            if rng.random() < prob:
                corrected = rng.choice(values)
                if direction == "up":
                    confirmed_cw = max(auto_cw, corrected)
                else:
                    confirmed_cw = min(auto_cw, corrected)

        records.append({
            "person": pid,
            "item": sid,
            "evidence_role": evidence_role,
            "auto_cw": round(auto_cw, 2),
            "confirmed_cw": round(confirmed_cw, 2),
            "deviation": round(abs(auto_cw - confirmed_cw), 3),
            "commit_count": len(commits),
        })

    deviations = [r["deviation"] for r in records]
    mad = sum(deviations) / len(deviations) if deviations else 0
    corrected_count = sum(1 for d in deviations if d > 0)

    gt = {
        "pi": pi_label,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats": {
            "total_records": len(records),
            "corrected_records": corrected_count,
            "correction_rate": round(corrected_count / len(records) * 100, 1) if records else 0,
            "mad": round(mad, 4),
            "total_deviation": round(sum(deviations), 4),
            "max_deviation": round(max(deviations), 3) if deviations else 0,
        },
        "records": records,
    }

    gt_path = DATA_DIR / f"ground_truth_{pi_label}.yaml"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(gt_path, "w") as f:
        yaml.dump(gt, f, default_flow_style=False, allow_unicode=True)

    return gt_path, len(records), gt["stats"]


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------


def print_iteration_summary(results, iteration_id, items_count, commit_count,
                            scenario_info=None):
    """Print a human-readable summary table for an iteration."""
    print(f"\n{'='*74}")
    print(f"  EDPA Iteration {iteration_id}")

    if scenario_info:
        factor = scenario_info["delivery_factor"]
        label = scenario_info["label"]
        desc = scenario_info["description"]
        planned_sp = scenario_info["planned_sp"]
        delivered_sp = scenario_info["delivered_sp"]
        planned_count = scenario_info["planned_stories"]
        delivered_count = scenario_info["delivered_stories"]
        spillover_count = scenario_info["spillover_count"]
        unplanned_count = scenario_info["unplanned_count"]
        capacity_sp = scenario_info["capacity_sp"]
        predictability = (delivered_sp / planned_sp * 100) if planned_sp > 0 else 0

        print(f"  Planning: {PLANNING_FACTOR:.0%} capacity -> "
              f"{planned_sp} SP planned (of {capacity_sp} SP capacity)")
        print(f"  Delivery: {factor:.0%} ({label} -- {desc})")
        print(f"  Planned:  {planned_count} stories ({planned_sp} SP)")
        print(f"  Delivered: {delivered_count} stories ({delivered_sp} SP)")
        if spillover_count > 0:
            spill_info = scenario_info.get("spillover_details", [])
            spill_str = ", ".join(
                f"{s['id']}, JS={s['js']}" for s in spill_info
            )
            print(f"  Spillover: {spillover_count} "
                  f"{'story' if spillover_count == 1 else 'stories'} "
                  f"({spill_str}) -> next iteration")
        else:
            print(f"  Spillover: 0")
        if unplanned_count > 0:
            unpl_info = scenario_info.get("unplanned_details", [])
            unpl_str = ", ".join(
                f"{s['id']}, JS={s['js']}" for s in unpl_info
            )
            print(f"  Unplanned: {unplanned_count} "
                  f"{'story' if unplanned_count == 1 else 'stories'} "
                  f"({unpl_str})")
        else:
            print(f"  Unplanned: 0")
        print(f"  FlowPredictability: {predictability:.1f}%")
    else:
        print(f"  Items: {items_count}  |  Commits: {commit_count}")

    print(f"{'='*74}")
    print(f"  {'Person':<25} {'Role':<10} {'Cap':>5} {'Derived':>8} {'Items':>6} {'OK':>4}")
    print(f"  {'-'*70}")

    team_cap = 0
    team_der = 0
    all_ok = True
    for r in results:
        ok_str = "OK" if r["invariant_ok"] else "FAIL"
        if not r["invariant_ok"]:
            all_ok = False
        team_cap += r["capacity"]
        team_der += r["total_derived"]
        print(
            f"  {r['name']:<25} {r['role']:<10} {r['capacity']:>4}h "
            f"{r['total_derived']:>7.1f}h {len(r['items']):>6} {ok_str:>4}"
        )

    print(f"  {'-'*70}")
    print(f"  {'TEAM':<25} {'':10} {team_cap:>4}h {team_der:>7.1f}h")
    print(f"  Invariants: {'ALL PASSED' if all_ok else 'FAILURES DETECTED'}")
    print()


def print_final_evaluation(all_iteration_data):
    """Print velocity trend, predictability, and CW accuracy across all iterations."""
    print(f"\n{'#'*74}")
    print(f"  FINAL EVALUATION")
    print(f"{'#'*74}")

    # ---- Predictability table (new) ----
    print(f"\n  Delivery Predictability:")
    print(f"  {'Iteration':<20} {'Planned':>8} {'Delivered':>10} "
          f"{'Predictability':>15} {'Scenario':>10}")
    print(f"  {'-'*68}")

    pi_groups = {}  # pi_num -> list of iteration entries
    for entry in all_iteration_data:
        iid = entry["iteration_id"]
        scenario = entry.get("scenario_info")
        if not scenario or entry["items_count"] == 0:
            # IP iterations
            continue

        # Determine PI number from iteration_id (e.g. PI-2026-1.2 -> 1)
        pi_num = int(iid.split("-")[2].split(".")[0])
        pi_groups.setdefault(pi_num, []).append(entry)

        planned_sp = scenario["planned_sp"]
        delivered_sp = scenario["delivered_sp"]
        pred = (delivered_sp / planned_sp * 100) if planned_sp > 0 else 0
        label = scenario["label"]

        print(f"  {iid:<20} {planned_sp:>5} SP  {delivered_sp:>7} SP  "
              f"{pred:>13.1f}%  {label:>10}")

    # Per-PI summary
    for pi_num in sorted(pi_groups.keys()):
        entries = pi_groups[pi_num]
        preds = []
        total_spillover = 0
        total_planned_stories = 0
        for e in entries:
            sc = e["scenario_info"]
            p_sp = sc["planned_sp"]
            d_sp = sc["delivered_sp"]
            if p_sp > 0:
                preds.append(d_sp / p_sp * 100)
            total_spillover += sc["spillover_count"]
            total_planned_stories += sc["planned_stories"]

        avg_pred = sum(preds) / len(preds) if preds else 0
        spill_rate = (total_spillover / total_planned_stories * 100) if total_planned_stories > 0 else 0
        print(f"  ---")
        print(f"  PI-{pi_num} avg predictability:   {avg_pred:>7.1f}%")
        print(f"  PI-{pi_num} spillover rate:       {spill_rate:>7.1f}%")
        print()

    # ---- Velocity trend (original, enhanced) ----
    print(f"\n  Velocity Trend (story points delivered per iteration):")
    print(f"  {'Iteration':<20} {'Stories':>8} {'Points':>8} {'Capacity':>10} {'Util%':>8}")
    print(f"  {'-'*60}")

    velocities = []
    for entry in all_iteration_data:
        iid = entry["iteration_id"]
        n_stories = entry["items_count"]
        total_js = entry["total_js"]
        team_derived = entry["team_derived"]
        util = (team_derived / TEAM_CAPACITY * 100) if TEAM_CAPACITY > 0 else 0
        velocities.append(total_js)
        print(f"  {iid:<20} {n_stories:>8} {total_js:>8} {TEAM_CAPACITY:>9}h {util:>7.1f}%")

    if velocities:
        avg_vel = sum(velocities) / len(velocities)
        if len(velocities) >= 2:
            std_dev = (sum((v - avg_vel) ** 2 for v in velocities) / len(velocities)) ** 0.5
            predictability = 1.0 - (std_dev / avg_vel) if avg_vel > 0 else 0
        else:
            std_dev = 0
            predictability = 1.0
        print(f"\n  Average velocity: {avg_vel:.1f} points/iteration")
        print(f"  Std deviation:    {std_dev:.1f}")
        print(f"  Predictability:   {predictability:.1%}")

    # Invariant summary
    print(f"\n  Invariant Validation Summary:")
    all_ok_count = sum(1 for e in all_iteration_data if e["all_passed"])
    total = len(all_iteration_data)
    print(f"  Iterations with all invariants passed: {all_ok_count}/{total}")

    print()


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------


def simulate_iteration(pi_num, iter_num, pi_start, story_ids, all_stories, rng,
                       dry_run=False, spillover_from_prev=None):
    """
    Simulate a single iteration with planning factor and delivery variance.

    spillover_from_prev: list of story IDs that spilled over from previous iteration

    Returns: dict with iteration results and metadata.
    """
    iteration_id = f"PI-2026-{pi_num}.{iter_num}"
    iter_start, iter_end = iteration_date_range(pi_start, iter_num)
    is_ip = iter_num == 5

    if is_ip:
        print(f"\n--- {iteration_id} (IP -- Innovation & Planning) ---")
        print(f"    Period: {iter_start.date()} to {iter_end.date()}")
        print(f"    No new stories. Hardening, retrospective, next PI planning.")
        if spillover_from_prev:
            print(f"    Note: {len(spillover_from_prev)} spillover stories from prev "
                  f"iteration carried to next PI backlog")
        if dry_run:
            return {
                "iteration_id": iteration_id,
                "items_count": 0,
                "total_js": 0,
                "team_derived": 0,
                "all_passed": True,
                "commit_count": 0,
                "spillover_out": [],
                "scenario_info": None,
            }

    # Merge spillover stories from previous iteration into this iteration's plan
    effective_story_ids = list(story_ids)
    if spillover_from_prev:
        # Add spillover at the start (highest priority)
        for sid in spillover_from_prev:
            if sid not in effective_story_ids:
                effective_story_ids.insert(0, sid)

    # Deduplicate
    effective_story_ids = list(dict.fromkeys(effective_story_ids))

    if is_ip:
        # IP iteration: no delivery work
        return {
            "iteration_id": iteration_id,
            "items_count": 0,
            "total_js": 0,
            "team_derived": 0,
            "all_passed": True,
            "commit_count": 0,
            "spillover_out": spillover_from_prev or [],
            "scenario_info": None,
        }

    # --- Determine scenario for this iteration ---
    delivery_factor, scenario_label, scenario_desc = get_scenario(pi_num, iter_num)

    # Calculate total planned SP
    planned_sp = 0
    for sid in effective_story_ids:
        s = all_stories.get(sid)
        if s:
            planned_sp += s["js"]

    # Capacity in SP (approximate: we use the raw planned SP from the backlog)
    # The planning_factor is conceptual -- the iteration_stories already represent
    # ~80% of what the team COULD take on. We report it for transparency.
    capacity_sp = round(planned_sp / PLANNING_FACTOR) if PLANNING_FACTOR > 0 else planned_sp

    # --- Apply delivery variance ---
    delivered_ids = list(effective_story_ids)
    spillover_ids = []
    unplanned_stories = []

    if delivery_factor < 1.0:
        # Under-delivery or good-but-not-perfect: some stories spill
        target_delivered_sp = round(planned_sp * delivery_factor)
        target_spill_sp = planned_sp - target_delivered_sp
        delivered_ids, spillover_ids = select_spillover_stories(
            effective_story_ids, all_stories, target_spill_sp, rng
        )
    elif delivery_factor > 1.0:
        # Over-delivery: team picks up extra unplanned stories
        extra_sp_budget = round(planned_sp * (delivery_factor - 1.0))
        added_sp = 0
        while added_sp < extra_sp_budget:
            unplanned = make_unplanned_story(pi_num, iter_num, rng, all_stories)
            if added_sp + unplanned["js"] > extra_sp_budget + 2:
                # Don't overshoot by too much
                break
            unplanned_stories.append(unplanned)
            # Register in all_stories so EDPA can find it
            all_stories[unplanned["id"]] = unplanned
            delivered_ids.append(unplanned["id"])
            added_sp += unplanned["js"]

    # Calculate actual delivered SP
    delivered_sp = sum(all_stories[sid]["js"] for sid in delivered_ids if sid in all_stories)
    spillover_sp = sum(all_stories[sid]["js"] for sid in spillover_ids if sid in all_stories)

    # Build scenario info dict
    spillover_details = []
    for sid in spillover_ids:
        s = all_stories.get(sid)
        if s:
            spillover_details.append({"id": sid, "js": s["js"], "title": s["title"]})

    unplanned_details = []
    for s in unplanned_stories:
        unplanned_details.append({"id": s["id"], "js": s["js"], "title": s["title"]})

    scenario_info = {
        "delivery_factor": delivery_factor,
        "label": scenario_label,
        "description": scenario_desc,
        "planning_factor": PLANNING_FACTOR,
        "capacity_sp": capacity_sp,
        "planned_sp": planned_sp,
        "delivered_sp": delivered_sp,
        "planned_stories": len(effective_story_ids),
        "delivered_stories": len(delivered_ids),
        "spillover_count": len(spillover_ids),
        "spillover_sp": spillover_sp,
        "spillover_details": spillover_details,
        "unplanned_count": len(unplanned_stories),
        "unplanned_details": unplanned_details,
    }

    print(f"\n--- {iteration_id} ---")
    print(f"    Period: {iter_start.date()} to {iter_end.date()}")
    print(f"    Planning: {PLANNING_FACTOR:.0%} capacity -> "
          f"{planned_sp} SP planned (of {capacity_sp} SP capacity)")
    print(f"    Delivery: {delivery_factor:.0%} "
          f"({scenario_label} -- {scenario_desc})")
    print(f"    Planned:  {len(effective_story_ids)} stories ({planned_sp} SP)")
    print(f"    Delivered: {len(delivered_ids)} stories ({delivered_sp} SP)")

    if spillover_ids:
        spill_str = ", ".join(
            f"{sid} JS={all_stories[sid]['js']}" for sid in spillover_ids if sid in all_stories
        )
        print(f"    Spillover: {len(spillover_ids)} "
              f"{'story' if len(spillover_ids) == 1 else 'stories'} "
              f"({spill_str}) -> next iteration")
    else:
        print(f"    Spillover: 0")

    if unplanned_stories:
        unpl_str = ", ".join(f"{s['id']} JS={s['js']}" for s in unplanned_stories)
        print(f"    Unplanned: {len(unplanned_stories)} "
              f"{'story' if len(unplanned_stories) == 1 else 'stories'} ({unpl_str})")
    else:
        print(f"    Unplanned: 0")

    if dry_run:
        # Show story details in dry-run
        for sid in delivered_ids:
            s = all_stories.get(sid)
            if s:
                tag = " [UNPLANNED]" if s.get("unplanned") else ""
                plan = determine_commit_plan(s, rng)
                print(f"      {sid} ({s['title']}) -- JS={s['js']}, "
                      f"owner={s['owner']}, commits={len(plan)}{tag}")
        for sid in spillover_ids:
            s = all_stories.get(sid)
            if s:
                print(f"      {sid} ({s['title']}) -- JS={s['js']}, "
                      f"status=Spillover")

        return {
            "iteration_id": iteration_id,
            "items_count": len(delivered_ids),
            "total_js": delivered_sp,
            "team_derived": 0,
            "all_passed": True,
            "commit_count": 0,
            "spillover_out": spillover_ids,
            "scenario_info": scenario_info,
        }

    # --- Live simulation: only commit delivered stories ---
    commit_log = []

    for sid in delivered_ids:
        story = all_stories.get(sid)
        if not story:
            print(f"    WARNING: Story {sid} not found, skipping")
            continue

        branch_name = f"feature/{sid}-{slugify(story['title'])}"

        # Create feature branch
        git(["checkout", "-b", branch_name, "main"])

        # Generate commit plan
        plan = determine_commit_plan(story, rng)

        for ci, (person_id, file_type, prefix) in enumerate(plan):
            # Generate file
            fpath = file_path_for(story, file_type, ci)
            full_path = ROOT / fpath
            full_path.parent.mkdir(parents=True, exist_ok=True)

            content = generate_file_content(story, file_type, ci)
            with open(full_path, "w") as f:
                f.write(content)

            # Stage and commit
            git(["add", fpath])
            commit_date = random_time_in_range(iter_start, iter_end, rng)
            date_str = commit_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            msg = f"{prefix}({sid}): {story['title']}"
            if ci > 0:
                msg += f" (part {ci + 1})"

            git_commit_as(person_id, msg, date_str)

            commit_log.append({
                "story_id": sid,
                "person_id": person_id,
                "file_type": file_type,
                "file_path": fpath,
                "date": date_str,
                "prefix": prefix,
            })

        # Merge branch back to main (simulate PR merge)
        git(["checkout", "main"])
        merge_date = random_time_in_range(
            iter_end - timedelta(days=2), iter_end, rng
        )
        merge_date_str = merge_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        merge_env = {
            "GIT_AUTHOR_DATE": merge_date_str,
            "GIT_COMMITTER_DATE": merge_date_str,
        }
        git(["merge", branch_name, "--no-ff", "-m",
             f"Merge branch '{branch_name}' -- {sid}: {story['title']}"],
            env_override=merge_env)

        # Clean up branch
        git(["branch", "-d", branch_name])

    print(f"    Commits generated: {len(commit_log)}")
    print(f"    Total delivered job size: {delivered_sp}")

    # Build items for EDPA engine -- ONLY delivered stories
    edpa_items = build_edpa_items(delivered_ids, all_stories, commit_log)

    # Run EDPA engine
    results = run_edpa_engine(edpa_items, iteration_id)

    if results is None:
        print(f"    WARNING: EDPA engine returned no results")
        return {
            "iteration_id": iteration_id,
            "items_count": len(delivered_ids),
            "total_js": delivered_sp,
            "team_derived": 0,
            "all_passed": False,
            "commit_count": len(commit_log),
            "spillover_out": spillover_ids,
            "scenario_info": scenario_info,
        }

    # Save reports
    report_path = save_iteration_report(
        results, iteration_id, edpa_items, commit_log, scenario_info=scenario_info
    )
    snapshot_path = save_snapshot(
        results, iteration_id, edpa_items, commit_log, scenario_info=scenario_info
    )
    print(f"    Report:   {report_path}")
    print(f"    Snapshot: {snapshot_path}")

    # Print summary
    print_iteration_summary(
        results, iteration_id, len(edpa_items), len(commit_log),
        scenario_info=scenario_info,
    )

    all_passed = all(r["invariant_ok"] for r in results if r["items"])
    team_derived = sum(r["total_derived"] for r in results)

    return {
        "iteration_id": iteration_id,
        "items_count": len(delivered_ids),
        "total_js": delivered_sp,
        "team_derived": team_derived,
        "all_passed": all_passed,
        "commit_count": len(commit_log),
        "commit_log": commit_log,
        "spillover_out": spillover_ids,
        "scenario_info": scenario_info,
    }


def simulate_pi(pi_num, epics, iteration_stories, pi_start, rng, dry_run=False):
    """Simulate an entire Planning Interval."""
    pi_label = f"PI-2026-{pi_num}"
    print(f"\n{'='*74}")
    print(f"  Planning Interval: {pi_label}")
    print(f"  Start: {pi_start.date()}")
    print(f"  Epics: {len(epics)}")
    features = sum(len(e['features']) for e in epics)
    all_stories = flatten_stories(epics)
    total_stories = len(all_stories)
    print(f"  Features: {features}  |  Stories: {total_stories}")
    print(f"  Planning factor: {PLANNING_FACTOR:.0%}")
    print(f"{'='*74}")

    iteration_data = []
    all_commit_logs = []
    spillover_carry = []  # Stories spilling from previous iteration

    for iter_num in range(1, 6):
        story_ids = iteration_stories.get(iter_num, [])
        # Deduplicate story IDs (in case of accidental dups in the plan)
        story_ids = list(dict.fromkeys(story_ids))

        result = simulate_iteration(
            pi_num, iter_num, pi_start, story_ids, all_stories, rng,
            dry_run=dry_run,
            spillover_from_prev=spillover_carry if spillover_carry else None,
        )
        iteration_data.append(result)

        # Carry spillover to next iteration
        spillover_carry = result.get("spillover_out", [])

        if "commit_log" in result:
            all_commit_logs.extend(result["commit_log"])

    # Between-PI evaluation with Karpathy calibration analysis
    if not dry_run and all_commit_logs:
        print(f"\n--- CW Evaluation for {pi_label} ---")
        gt_path, n_records, gt_stats = generate_ground_truth(all_commit_logs, all_stories, pi_label)
        print(f"    Ground truth: {gt_path} ({n_records} records)")
        print(f"    Corrected by team: {gt_stats['corrected_records']}/{n_records} ({gt_stats['correction_rate']}%)")
        print(f"    MAD (auto vs confirmed): {gt_stats['mad']:.4f}")
        print(f"    Total deviation: {gt_stats['total_deviation']:.4f}")
        print(f"    Max single deviation: {gt_stats['max_deviation']:.3f}")
        print()

        if n_records >= 20:
            print(f"    === Karpathy Calibration Analysis ===")
            if gt_stats['mad'] > 0.05:
                print(f"    MAD={gt_stats['mad']:.4f} -- calibration RECOMMENDED")
                print(f"    Systematic biases detected:")
                with open(gt_path) as f:
                    gt_data = yaml.safe_load(f)
                role_bias = {}
                for rec in gt_data["records"]:
                    if rec["deviation"] > 0:
                        pid = rec["person"]
                        member = TEAM_BY_ID.get(pid, {})
                        role = member.get("role", "?")
                        role_bias.setdefault(role, []).append(
                            rec["confirmed_cw"] - rec["auto_cw"]
                        )
                for role, biases in sorted(role_bias.items()):
                    avg_bias = sum(biases) / len(biases)
                    direction = "UNDERVALUED (increase CW)" if avg_bias > 0 else "OVERVALUED (decrease CW)"
                    print(f"      {role}: avg bias {avg_bias:+.3f} -- {direction} ({len(biases)} corrections)")
            else:
                print(f"    MAD={gt_stats['mad']:.4f} -- heuristic is WELL CALIBRATED")
        else:
            print(f"    Insufficient records for evaluation (need >=20, got {n_records})")
    elif dry_run:
        print(f"\n--- CW Evaluation for {pi_label} (dry-run: skipped) ---")

    return iteration_data


def main():
    parser = argparse.ArgumentParser(
        description="EDPA Full Simulation -- Medical Platform (kashealth.cz)",
    )
    parser.add_argument(
        "--pi", choices=["1", "2", "all"], default="all",
        help="Which PI to simulate (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print simulation plan without executing git operations",
    )
    parser.add_argument(
        "--seed", type=int, default=RANDOM_SEED,
        help=f"Random seed for reproducibility (default: {RANDOM_SEED})",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)

    print("=" * 74)
    print("  EDPA Simulation -- Medical Platform (kashealth.cz)")
    print(f"  Methodology: EDPA v1.0.0")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"  PI scope: {args.pi}")
    print(f"  Team: {len(TEAM)} members, {TEAM_CAPACITY}h/iteration")
    print(f"  Planning factor: {PLANNING_FACTOR:.0%}")
    print(f"  Seed: {args.seed}")
    print("=" * 74)

    if args.dry_run:
        # Show scenario assignment overview
        print(f"\n  Scenario Assignment:")
        print(f"  {'Iteration':<20} {'Factor':>8} {'Label':>8} {'Description'}")
        print(f"  {'-'*70}")
        for (pi, it), idx in sorted(ITERATION_SCENARIO_MAP.items()):
            factor, label, desc = DELIVERY_SCENARIOS[idx]
            iid = f"PI-2026-{pi}.{it}"
            print(f"  {iid:<20} {factor:>7.0%} {label:>8}   {desc}")
        print()

    if not args.dry_run:
        ensure_main_branch()

    all_iteration_data = []

    # PI-1
    if args.pi in ("1", "all"):
        pi1_data = simulate_pi(
            pi_num=1,
            epics=PI1_EPICS,
            iteration_stories=PI1_ITERATION_STORIES,
            pi_start=PI1_START,
            rng=rng,
            dry_run=args.dry_run,
        )
        all_iteration_data.extend(pi1_data)

    # PI-2
    if args.pi in ("2", "all"):
        pi2_start = PI1_START + timedelta(days=5 * ITERATION_DAYS)  # After PI-1
        pi2_data = simulate_pi(
            pi_num=2,
            epics=PI2_EPICS,
            iteration_stories=PI2_ITERATION_STORIES,
            pi_start=pi2_start,
            rng=rng,
            dry_run=args.dry_run,
        )
        all_iteration_data.extend(pi2_data)

    # Final evaluation
    if not args.dry_run:
        print_final_evaluation(all_iteration_data)
    else:
        print(f"\n{'#'*74}")
        print(f"  DRY RUN SUMMARY")
        print(f"{'#'*74}")
        total_stories = sum(e["items_count"] for e in all_iteration_data)
        total_js = sum(e["total_js"] for e in all_iteration_data)
        delivery_iters = [e for e in all_iteration_data if e["items_count"] > 0]
        ip_iters = [e for e in all_iteration_data if e["items_count"] == 0]

        total_spillover = sum(
            len(e.get("spillover_out", [])) for e in all_iteration_data
        )
        print(f"  Delivery iterations: {len(delivery_iters)}")
        print(f"  IP iterations:       {len(ip_iters)}")
        print(f"  Total stories delivered: {total_stories}")
        print(f"  Total job size delivered: {total_js} points")
        print(f"  Total spillover stories: {total_spillover}")
        print(f"  Team capacity/iter:  {TEAM_CAPACITY}h")
        print(f"  Planning factor:     {PLANNING_FACTOR:.0%}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
