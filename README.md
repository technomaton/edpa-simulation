# EDPA Simulation -- Medical Platform (kashealth.cz)

Simulation of the Evidence-Driven Proportional Allocation (EDPA) v1.0.0 methodology
applied to the Medical Platform & Datovy e-shop project.

## Overview

This repository simulates 2 Planning Intervals (PIs), each containing 5 iterations
(4 delivery + 1 IP), using a 2-week cadence (Klasicka 2/10). The simulation generates
realistic Git history, runs the EDPA engine per iteration, and produces audit-ready
reports and snapshots.

## Team

| ID       | Name               | Role      | FTE  | Capacity/iter |
|----------|--------------------|-----------|------|---------------|
| novak    | Jan Novak          | BO        | 0.30 | 24h           |
| kralova  | Marie Kralova      | PM        | 0.50 | 40h           |
| urbanek  | Jaroslav Urbanek   | Arch      | 0.70 | 56h           |
| svoboda  | Petr Svoboda       | Dev       | 1.00 | 80h           |
| cerny    | Tomas Cerny        | Dev       | 1.00 | 80h           |
| tuma     | Ondrej Tuma        | DevSecOps | 0.80 | 64h           |
| nemcova  | Katerina Nemcova   | QA        | 1.20 | 96h           |

## Quick Start

```bash
# Dry run -- print simulation plan without executing
python scripts/simulate.py --dry-run

# Simulate PI-1 only
python scripts/simulate.py --pi 1

# Simulate PI-2 only
python scripts/simulate.py --pi 2

# Simulate both PIs
python scripts/simulate.py --pi all
```

## Project Structure

```
edpa-simulation/
  config/          -- capacity, project, and CW heuristics configuration
  scripts/         -- EDPA engine, CW evaluator, and simulation script
  data/            -- ground truth and intermediate data
  reports/         -- per-iteration EDPA reports and vykazy
  snapshots/       -- frozen iteration snapshots
```

## Methodology

- EDPA v1.0.0
- Calculation mode: simple
- Audit mode: full
- Registration: CZ.01.01.01/01/24_062/0007440
- Program: OP TAK
