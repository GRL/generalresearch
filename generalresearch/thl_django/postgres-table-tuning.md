
## PostgreSQL Table Storage & Autovacuum Tuning

For certain very large, append-only tables, we want to adjust some setting
to get the vacuum and analyze to run within a reasonable frequency.

### Manual
First, manually analyze all tables. Note, ensure you do this with a user that has
permission, or it will just do nothing. Verify with `ANALYZE VERBOSE event_bribe;`
```postgresql
ANALYZE 
```

### Autoanalyze
For tables that are large and typically append only, the default auto-analyze config
results in analyze almost never getting run. e.g. if a table has 10 million rows,
and the autovacuum_analyze_scale_factor default is 10%, then 1 million rows
have to be inserted or updated before the auto-analyze runs.

```postgresql
ALTER TABLE marketplace_userquestionanswer SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE ledger_transactionmetadata SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE ledger_entry SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE ledger_transaction SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE userhealth_iphistory SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE marketplace_userprofileknowledgeitem SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE userhealth_auditlog SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE thl_ipinformation SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE marketplace_userprofileknowledgenumerical SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE marketplace_userprofileknowledgetext SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 50000, autovacuum_vacuum_scale_factor = 0.2);

ALTER TABLE thl_wall SET (
    autovacuum_analyze_scale_factor = 0.002, autovacuum_analyze_threshold = 10000, 
    autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 5000,
    fillfactor = 85
);
ALTER TABLE thl_session SET (
    autovacuum_analyze_scale_factor = 0.002, autovacuum_analyze_threshold = 10000,
    autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 5000,
    fillfactor = 85
);
ALTER TABLE thl_user SET (
    autovacuum_analyze_scale_factor = 0.002, autovacuum_analyze_threshold = 10000, 
    autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 5000,
    fillfactor = 85
);

```

### SurveyStats

```postgresql
ALTER TABLE marketplace_surveystat SET (fillfactor = 60);
ALTER TABLE marketplace_surveystat SET (
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50000,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50000
);


```