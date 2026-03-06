## Suggested Postgres Settings

**/etc/postgresql/18/main/postgresql.conf**

For both primary and any replicas config
```text

shared_buffers = 2048MB  # Default 128MB (25% of total RAM)
effective_cache_size = 6GB  # (~75% of total RAM)
work_mem = 12MB  # Default 4MB
maintenance_work_mem = 256MB  # Default 64MB

# On fast ssds
random_page_cost = 1.3
effective_io_concurrency = 50

statement_timeout = 60min  # default unlimited
lock_timeout = 60s  # default unlimited
idle_in_transaction_session_timeout = 10min  # default unlimited

min_wal_size = 1GB
max_wal_size = 4GB
max_parallel_workers_per_gather = 4
max_parallel_maintenance_workers = 4
max_parallel_workers = 8
```

## Suggested Postgres Settings for Read Replica instances

In the primary's config
```text
wal_level=replica
max_wal_senders=5
hot_standby=on
wal_sender_timeout=10s
wal_keep_size=4GB
```

In the replica's config
```text
hot_standby_feedback = on
```

```postgresql
--- To run before pg_basebackup
SELECT pg_create_physical_replication_slot('xxx_dr_001');

--- To run if pg_basebackup fails
SELECT pg_drop_replication_slot('xxx_dr_001');
```

```text
pg_basebackup -h ___ -D /var/lib/postgresql/18/main -U repl____ -P -v -R -X stream -C -S 'xxx_dr_001'
```

## Setting up Percona Monitoring

Install the pmm-client. As of 2025-12-16 Percona doesn't have 3.5 on the Trixie
source, so we use bookworm still.

```bash
sudo apt-get install gpgv lsb-release gnupg curl vim -y
wget https://repo.percona.com/apt/percona-release_latest.generic_all.deb
sudo dpkg -i percona-release_latest.generic_all.deb
sudo percona-release enable pmm3-client

sed -i 's/trixie/bookworm/g' /etc/apt/sources.list.d/percona-pmm3-client-release.list
sed -i 's/trixie/bookworm/g' /etc/apt/sources.list.d/percona-prel-release.list
sed -i 's/trixie/bookworm/g' /etc/apt/sources.list.d/percona-telemetry-release.list

sudo apt update
sudo apt install -y pmm-client
pmm-admin --version
```

With the base pmm-client installed, go ahead and setup the enhanced 
pg_stat_monitor system.

```bash
percona-release setup ppg-18
apt-get install percona-pg-stat-monitor18
```

Add these to the postgresql server's configuration file

`vim /etc/postgresql/18/main/postgresql.conf`

```text
shared_preload_libraries = 'pg_stat_monitor'
# Options: https://docs.percona.com/pg-stat-monitor/configuration.html#parameters-description
pg_stat_monitor.pgsm_query_max_len = 2048
pg_stat_monitor.pgsm_normalized_query = 1
pg_stat_monitor.pgsm_track = all
pg_stat_monitor.pgsm_enable_query_plan = 0
pg_stat_monitor.pgsm_extract_comments = 1
pg_stat_monitor.pgsm_track_planning = 0
```

Setup a `pmm` reporting user for the database, and make sure to explicitly
allow it localhost connection permission.

```
vim /etc/postgresql/18/main/pg_hba.conf 
host    all             pmm             127.0.0.1/32            scram-sha-256
```

Create the extension, confirm it's working, and make sure the pmm user's
permissions are good.

`sudo -u postgres psql`:

```postgresql
ALTER SYSTEM SET pg_stat_monitor.pgsm_enable_query_plan = off;
SELECT pg_reload_conf();

-- Only create on the Master instance
CREATE EXTENSION IF NOT EXISTS pg_stat_monitor;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SELECT * FROM pg_available_extensions WHERE name = 'pg_stat_monitor';
\dx pg_stat_monitor
SELECT COUNT(*) FROM pg_stat_monitor; 

CREATE USER pmm WITH PASSWORD '______';
GRANT pg_monitor TO pmm;
GRANT CONNECT ON DATABASE postgres TO pmm;

\c postgres
GRANT pg_monitor TO pmm;

\c ______
GRANT CONNECT ON DATABASE ______ TO pmm;
GRANT USAGE ON SCHEMA public TO pmm;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pmm;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pmm;
```

```

pmm-admin config --server-insecure-tls --server-url=https://______:______@______.internal:443
pmm-admin remove postgresql ______-postgresql
pmm-admin add postgresql --username=pmm --password='______' --host=127.0.0.1 --port=5432 --service-name=______-postgresql-__ --query-source=pgstatmonitor

```