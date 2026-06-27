# Tâches — Complétion Projet 01 Fraude Temps Réel

## Chantier 1 — Schema Registry Avro
- [ ] Créer `src/schemas/transaction.avsc`
- [ ] Créer `src/schemas/fraud_alert.avsc`
- [ ] Modifier `docker-compose.yml` — ajouter schema-registry
- [ ] Modifier `transaction_generator.py` — sérialisation Avro
- [ ] Modifier `fraud_detection.py` — désérialisation Avro
- [ ] Modifier `requirements.txt` — ajouter dépendances Avro

## Chantier 2 — Règle CEP : Pattern de Carding
- [ ] Modifier `fraud_detection.py` — ajouter `check_carding_pattern()`
- [ ] Modifier `fraud_detection_flink.py` — ajouter règle SQL carding
- [ ] Modifier `test_rules.py` — ajouter `TestCardingPattern`

## Chantier 3 — API REST Enrichie
- [ ] Modifier `main.py` — ajouter endpoints stats, card, metrics

## Chantier 4 — Tests d'Intégration
- [ ] Créer `src/tests/test_integration.py`

## Chantier 5 — Script de Benchmark
- [ ] Créer `scripts/benchmark.py`
- [ ] Créer `PERFORMANCE.md`

## Chantier 6 — CI/CD Pipeline
- [ ] Créer `.github/workflows/ci.yml`
- [ ] Créer `.ruff.toml`

## Chantier 7 — Documentation Finale
- [ ] Mettre à jour `architecture.md`
- [ ] Mettre à jour `README.md`
