#!/usr/bin/env python3
"""
EDPA Full Simulation -- Medical Platform (kashealth.cz)

Simulates 2 Planning Intervals of EDPA-managed delivery with realistic Git history,
runs the EDPA engine per iteration, and produces audit-ready reports and snapshots.

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
# Team definition (loaded from config but also inlined for commit authoring)
# ---------------------------------------------------------------------------

TEAM = [
    {
        "id": "novak", "name": "Jan Novák", "role": "BO",
        "fte": 0.30, "capacity": 24, "email": "novak@kashealth.cz",
        "team": "Management",
    },
    {
        "id": "kralova", "name": "Marie Králová", "role": "PM",
        "fte": 0.50, "capacity": 40, "email": "kralova@kashealth.cz",
        "team": "Management",
    },
    {
        "id": "urbanek", "name": "Jaroslav Urbánek", "role": "Arch",
        "fte": 0.70, "capacity": 56, "email": "urbanek@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "svoboda", "name": "Petr Svoboda", "role": "Dev",
        "fte": 1.00, "capacity": 80, "email": "svoboda@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "cerny", "name": "Tomáš Černý", "role": "Dev",
        "fte": 1.00, "capacity": 80, "email": "cerny@kashealth.cz",
        "team": "Core",
    },
    {
        "id": "tuma", "name": "Ondřej Tůma", "role": "DevSecOps",
        "fte": 0.80, "capacity": 64, "email": "tuma@kashealth.cz",
        "team": "Platform",
    },
    {
        "id": "nemcova", "name": "Kateřina Němcová", "role": "QA",
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
        "id": "E-10", "title": "Anonymizační modul", "js": 13,
        "features": [
            {
                "id": "F-100", "title": "K-anonymity engine", "js": 8,
                "stories": [
                    {"id": "S-1001", "title": "Implementace QI detekce", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek", "nemcova"]},
                    {"id": "S-1002", "title": "Generalizační hierarchie", "js": 3,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-1003", "title": "K-anonymity validátor", "js": 5,
                     "owner": "cerny", "contributors": ["nemcova"]},
                    {"id": "S-1004", "title": "Unit testy anonymizace", "js": 2,
                     "owner": "nemcova", "contributors": ["svoboda"]},
                ],
            },
            {
                "id": "F-101", "title": "L-diversity rozšíření", "js": 5,
                "stories": [
                    {"id": "S-1005", "title": "L-diversity algoritmus", "js": 5,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-1006", "title": "Konfigurace citlivých atributů", "js": 3,
                     "owner": "svoboda", "contributors": ["tuma"]},
                    {"id": "S-1007", "title": "Benchmark výkonu anonymizace", "js": 2,
                     "owner": "nemcova", "contributors": ["cerny"]},
                ],
            },
            {
                "id": "F-102", "title": "Anonymizační pipeline", "js": 8,
                "stories": [
                    {"id": "S-1008", "title": "Celery task pro anonymizaci", "js": 5,
                     "owner": "tuma", "contributors": ["svoboda", "cerny"]},
                    {"id": "S-1009", "title": "Monitoring pipeline metriky", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                    {"id": "S-1010", "title": "Error handling a retry logika", "js": 3,
                     "owner": "cerny", "contributors": ["tuma"]},
                    {"id": "S-1011", "title": "Integrační testy pipeline", "js": 3,
                     "owner": "nemcova", "contributors": ["tuma", "svoboda"]},
                ],
            },
        ],
    },
    {
        "id": "E-11", "title": "Datový e-shop API", "js": 8,
        "features": [
            {
                "id": "F-110", "title": "Katalog datasetů", "js": 5,
                "stories": [
                    {"id": "S-1101", "title": "REST endpoint /datasets", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek"]},
                    {"id": "S-1102", "title": "Filtrování a stránkování", "js": 3,
                     "owner": "cerny", "contributors": ["svoboda"]},
                    {"id": "S-1103", "title": "OpenAPI specifikace katalogu", "js": 2,
                     "owner": "urbanek", "contributors": ["kralova"]},
                ],
            },
            {
                "id": "F-111", "title": "Objednávkový proces", "js": 5,
                "stories": [
                    {"id": "S-1104", "title": "Nákupní košík API", "js": 5,
                     "owner": "cerny", "contributors": ["svoboda"]},
                    {"id": "S-1105", "title": "Platební integrace mock", "js": 3,
                     "owner": "svoboda", "contributors": ["tuma"]},
                    {"id": "S-1106", "title": "E2E testy objednávky", "js": 3,
                     "owner": "nemcova", "contributors": ["cerny", "svoboda"]},
                ],
            },
            {
                "id": "F-112", "title": "Autorizace a přístupová práva", "js": 5,
                "stories": [
                    {"id": "S-1107", "title": "RBAC model pro e-shop", "js": 5,
                     "owner": "tuma", "contributors": ["urbanek"]},
                    {"id": "S-1108", "title": "JWT token management", "js": 3,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-1109", "title": "Audit log přístupů", "js": 3,
                     "owner": "tuma", "contributors": ["nemcova"]},
                ],
            },
        ],
    },
    {
        "id": "E-12", "title": "OMOP CDM integrace", "js": 5,
        "features": [
            {
                "id": "F-120", "title": "OMOP schéma migrace", "js": 5,
                "stories": [
                    {"id": "S-1201", "title": "Alembic migrace OMOP tabulek", "js": 5,
                     "owner": "urbanek", "contributors": ["svoboda"]},
                    {"id": "S-1202", "title": "ETL transformace zdrojových dat", "js": 5,
                     "owner": "svoboda", "contributors": ["urbanek", "cerny"]},
                    {"id": "S-1203", "title": "Validace OMOP integrity", "js": 3,
                     "owner": "nemcova", "contributors": ["urbanek"]},
                ],
            },
            {
                "id": "F-121", "title": "OMOP vocabulář služba", "js": 3,
                "stories": [
                    {"id": "S-1204", "title": "Vocabulary lookup API", "js": 3,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-1205", "title": "Concept mapping nástroj", "js": 3,
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
        "id": "E-20", "title": "Pokročilá analytika", "js": 8,
        "features": [
            {
                "id": "F-200", "title": "Statistický engine", "js": 5,
                "stories": [
                    {"id": "S-2001", "title": "Deskriptivní statistiky modul", "js": 5,
                     "owner": "cerny", "contributors": ["urbanek"]},
                    {"id": "S-2002", "title": "Korelační analýza služba", "js": 5,
                     "owner": "svoboda", "contributors": ["cerny"]},
                    {"id": "S-2003", "title": "Regresní model API", "js": 5,
                     "owner": "cerny", "contributors": ["svoboda", "urbanek"]},
                    {"id": "S-2004", "title": "Statistické testy modul", "js": 3,
                     "owner": "nemcova", "contributors": ["cerny"]},
                ],
            },
            {
                "id": "F-201", "title": "Vizualizační služba", "js": 5,
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
                "id": "F-202", "title": "Analytický sandbox", "js": 5,
                "stories": [
                    {"id": "S-2008", "title": "Jupyter notebook integrace", "js": 5,
                     "owner": "tuma", "contributors": ["urbanek", "svoboda"]},
                    {"id": "S-2009", "title": "Sandbox izolace a limity", "js": 3,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-2010", "title": "Sdílení notebooků API", "js": 3,
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
                    {"id": "S-2106", "title": "Automatická expirační služba", "js": 5,
                     "owner": "tuma", "contributors": ["cerny"]},
                    {"id": "S-2107", "title": "Retention reporting dashboard", "js": 3,
                     "owner": "svoboda", "contributors": ["nemcova"]},
                ],
            },
        ],
    },
    {
        "id": "E-22", "title": "Platformová stabilizace", "js": 5,
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


def save_iteration_report(results, iteration_id, items, commit_log):
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


def save_snapshot(results, iteration_id, items, commit_log):
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

    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    return snapshot_path


# ---------------------------------------------------------------------------
# Ground truth generation for CW evaluation
# ---------------------------------------------------------------------------


def generate_ground_truth(commit_log, all_stories, pi_label):
    """Generate ground truth YAML for CW calibration evaluation."""
    records = []

    # Group commits by (person, story)
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

        # Determine evidence role
        if pid == owner:
            evidence_role = "owner"
            confirmed_cw = 1.0
        elif pid in contributors:
            evidence_role = "key"
            confirmed_cw = 0.6
        else:
            member = TEAM_BY_ID.get(pid, {})
            role = member.get("role", "")
            if role in ("Arch", "QA"):
                evidence_role = "reviewer"
                confirmed_cw = 0.25
            else:
                evidence_role = "consulted"
                confirmed_cw = 0.15

        records.append({
            "person": pid,
            "item": sid,
            "evidence_role": evidence_role,
            "confirmed_cw": confirmed_cw,
            "commit_count": len(commits),
        })

    gt = {
        "pi": pi_label,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }

    gt_path = DATA_DIR / f"ground_truth_{pi_label}.yaml"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(gt_path, "w") as f:
        yaml.dump(gt, f, default_flow_style=False, allow_unicode=True)

    return gt_path, len(records)


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------


def print_iteration_summary(results, iteration_id, items_count, commit_count):
    """Print a human-readable summary table for an iteration."""
    print(f"\n{'='*74}")
    print(f"  EDPA Iteration {iteration_id}")
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

    # Velocity trend
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
                       dry_run=False):
    """
    Simulate a single iteration.

    Returns: dict with iteration results and metadata.
    """
    iteration_id = f"PI-2026-{pi_num}.{iter_num}"
    iter_start, iter_end = iteration_date_range(pi_start, iter_num)
    is_ip = iter_num == 5

    if is_ip:
        print(f"\n--- {iteration_id} (IP -- Innovation & Planning) ---")
        print(f"    Period: {iter_start.date()} to {iter_end.date()}")
        print(f"    No new stories. Hardening, retrospective, next PI planning.")
        if dry_run:
            return {
                "iteration_id": iteration_id,
                "items_count": 0,
                "total_js": 0,
                "team_derived": 0,
                "all_passed": True,
                "commit_count": 0,
            }
    else:
        print(f"\n--- {iteration_id} ---")
        print(f"    Period: {iter_start.date()} to {iter_end.date()}")
        print(f"    Stories: {len(story_ids)}")

    if dry_run:
        total_js = 0
        for sid in story_ids:
            s = all_stories.get(sid)
            if s:
                total_js += s["js"]
                plan = determine_commit_plan(s, rng)
                print(f"      {sid} ({s['title']}) -- JS={s['js']}, "
                      f"owner={s['owner']}, commits={len(plan)}")
        print(f"    Total job size: {total_js}")
        return {
            "iteration_id": iteration_id,
            "items_count": len(story_ids),
            "total_js": total_js,
            "team_derived": 0,
            "all_passed": True,
            "commit_count": 0,
        }

    # --- Live simulation ---
    commit_log = []
    total_js = 0

    for sid in story_ids:
        story = all_stories.get(sid)
        if not story:
            print(f"    WARNING: Story {sid} not found, skipping")
            continue

        total_js += story["js"]
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
    print(f"    Total job size: {total_js}")

    # Build items for EDPA engine
    edpa_items = build_edpa_items(story_ids, all_stories, commit_log)

    # Run EDPA engine
    results = run_edpa_engine(edpa_items, iteration_id)

    if results is None:
        print(f"    WARNING: EDPA engine returned no results")
        return {
            "iteration_id": iteration_id,
            "items_count": len(story_ids),
            "total_js": total_js,
            "team_derived": 0,
            "all_passed": False,
            "commit_count": len(commit_log),
        }

    # Save reports
    report_path = save_iteration_report(results, iteration_id, edpa_items, commit_log)
    snapshot_path = save_snapshot(results, iteration_id, edpa_items, commit_log)
    print(f"    Report:   {report_path}")
    print(f"    Snapshot: {snapshot_path}")

    # Print summary
    print_iteration_summary(results, iteration_id, len(edpa_items), len(commit_log))

    all_passed = all(r["invariant_ok"] for r in results if r["items"])
    team_derived = sum(r["total_derived"] for r in results)

    return {
        "iteration_id": iteration_id,
        "items_count": len(story_ids),
        "total_js": total_js,
        "team_derived": team_derived,
        "all_passed": all_passed,
        "commit_count": len(commit_log),
        "commit_log": commit_log,
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
    print(f"{'='*74}")

    iteration_data = []
    all_commit_logs = []

    for iter_num in range(1, 6):
        story_ids = iteration_stories.get(iter_num, [])
        # Deduplicate story IDs (in case of accidental dups in the plan)
        story_ids = list(dict.fromkeys(story_ids))

        result = simulate_iteration(
            pi_num, iter_num, pi_start, story_ids, all_stories, rng,
            dry_run=dry_run,
        )
        iteration_data.append(result)

        if "commit_log" in result:
            all_commit_logs.extend(result["commit_log"])

    # Between-PI evaluation
    if not dry_run and all_commit_logs:
        print(f"\n--- CW Evaluation for {pi_label} ---")
        gt_path, n_records = generate_ground_truth(all_commit_logs, all_stories, pi_label)
        print(f"    Ground truth: {gt_path} ({n_records} records)")

        if n_records >= 20:
            # Run evaluate_cw.py
            heuristics_path = CONFIG_DIR / "cw_heuristics.yaml"
            eval_script = ROOT / "scripts" / "evaluate_cw.py"
            result = subprocess.run(
                [sys.executable, str(eval_script),
                 "--ground-truth", str(gt_path),
                 "--heuristics", str(heuristics_path)],
                capture_output=True, text=True, cwd=str(ROOT),
            )
            print(f"    Evaluation output:")
            for line in result.stdout.strip().split("\n"):
                print(f"      {line}")
            if result.returncode != 0 and result.stderr:
                print(f"    Evaluation stderr: {result.stderr.strip()}")
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
    print(f"  Seed: {args.seed}")
    print("=" * 74)

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
        print(f"  Delivery iterations: {len(delivery_iters)}")
        print(f"  IP iterations:       {len(ip_iters)}")
        print(f"  Total stories:       {total_stories}")
        print(f"  Total job size:      {total_js} points")
        print(f"  Team capacity/iter:  {TEAM_CAPACITY}h")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
