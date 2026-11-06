# AGENTS.md

Rules for agents that change this repository. Humans: see [CONTRIBUTING](CONTRIBUTING.md).

## verify-orphan-sweep

- The facts in `.agents/skills/verify-orphan-sweep/facts/facts.go` pin a call's presence and position and the named text of a guard's condition only. Do not make a fact prove a guard's strength by matching code text (a condition, a route, a closure). The fingerprints in `model/fingerprints.txt` and the behavior tests guard the rest.
