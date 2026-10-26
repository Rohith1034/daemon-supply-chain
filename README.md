# Demon Supply Chain Platform

End-to-end supply chain data engineering and AI/ML platform.

## Architecture

Simulator
↓
PostgreSQL
↓
Outbox Pattern
↓
Kafka
↓
GCS Bronze
↓
Spark
↓
BigQuery
↓
ML Models


## Services

- FastAPI
- PostgreSQL
- Kafka
- Airflow
- Spark
- GCP Storage
- BigQuery


## Domains

- Supplier
- Procurement
- Inventory
- Warehouse
- Orders
- Transportation
- Delivery

## Local Python Setup

Use Python 3.13 for local development. The pinned `psycopg2-binary==2.9.12` dependency does not provide a wheel for Python 3.15, so pip falls back to compiling it from source and fails on macOS without PostgreSQL's `pg_config`. The 3.13.17 file also selects 3.13 for pyenv users.

On macOS, install and use Python 3.13 to create the project environment:

```bash
brew install python@3.13
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Independent Simulator Runs

Apply `migrations/002_simulation_runs.sql` to PostgreSQL before starting the API. Each independent endpoint can then be called without a request body; PostgreSQL supplies the simulation ID, correlation ID, and default timestamp, and stores run status and results in `simulation_runs` instead of creating files under `output/simulation_runs`.

Each request runs one lifecycle cycle. A scheduler can call the inbound endpoint during the 00:00-12:00 window and the outbound endpoint during the 06:00-18:00 window, up to 120-150 times per window. The API serializes cycles against the shared simulator database; scheduling and cycle counts remain the caller's responsibility.

Run each lifecycle independently from the repository root:

Airflow mounts the `simulator` package so its DAGs can import it. For Google Cloud Storage access, the Compose setup mounts a service-account JSON key read-only from `credentials/daemon-supply-chain-dc9d68e4bf2c.json`; set `GOOGLE_APPLICATION_CREDENTIALS_HOST` in `.env` if the key is stored elsewhere. Keep the key out of version control and grant its service account only the required bucket access.

For Compose runs, configure the simulator database using `SIMULATOR_DB_HOST`, `SIMULATOR_DB_PORT`, `SIMULATOR_DB_NAME`, `SIMULATOR_DB_USER`, and `SIMULATOR_DB_PASSWORD` in `.env`. The API and Airflow tasks read the same settings. Inside Docker, `localhost` is the container itself; use `host.docker.internal` for PostgreSQL installed on the Docker host, or the reachable private address for a remote database. An SSH tunnel on your workstation (such as `127.0.0.1:55432`) is only for workstation clients; containers on the VM must connect directly to the VM PostgreSQL listener (typically `host.docker.internal:5432`). The producer opens its database connection only when its task runs.

```powershell
gcloud compute ssh daemon-vm --zone=asia-south1-c --ssh-flag="-N" --ssh-flag="-L 55432:127.0.0.1:5432"

"C:\Program Files\PostgreSQL\18\bin\psql.exe" -h 127.0.0.1 -p 55432 -U supplychain_app -d supply_chain 

docker compose up -d --build --force-recreate simulator-api airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer

$base = "http://127.0.0.1:8000"
Invoke-RestMethod -Method Post -Uri "$base/simulation/inbound"
Invoke-RestMethod -Method Post -Uri "$base/simulation/outbound"
Invoke-RestMethod -Method Post -Uri "$base/simulation/transportation"


$base = "http://127.0.0.1:8000"
$flow = "inbound" # use "inbound" for inbound cycles

for ($i = 1; $i -le 30; $i++) {
    $result = Invoke-RestMethod -Method Post -Uri "$base/simulation/$flow"
    if ($result.status -ne "SUCCESS") {
        throw "$flow cycle $i failed: $($result.message)"
    }
    Write-Host "$i/30 $flow succeeded: $($result.simulation_id)"
}

$base = "http://127.0.0.1:8000"
$flow = "outbound" # use "inbound" for inbound cycles

for ($i = 1; $i -le 30; $i++) {
    $result = Invoke-RestMethod -Method Post -Uri "$base/simulation/$flow"
    if ($result.status -ne "SUCCESS") {
        throw "$flow cycle $i failed: $($result.message)"
    }
    Write-Host "$i/30 $flow succeeded: $($result.simulation_id)"
}
```

To reset transactional tables and run all three flows three times, use:

```powershell
.\.venv\Scripts\python.exe scripts\run_independent_flow_validation.py 3
```

The validation runner clears only the reviewed transactional-table allowlist before each flow and verifies that required master-table counts remain unchanged. It preserves `product_supplier_mapping` as reference data.

## Deploying on a GCE VM

This deployment preserves the existing `supply_chain` data in PostgreSQL installed on the VM. The Compose stack runs the simulator API, Airflow, a separate PostgreSQL database for Airflow metadata, Redis, and an Apache Spark standalone cluster with one master and three workers; the Compose `postgres` service is not the simulator database. PostgreSQL on the VM is managed by systemd and should be enabled at boot. Confluent Cloud and Google Cloud Storage remain external services; Spark is available as a cluster but is not wired into the existing DAGs or data pipeline.

The inbound DAG runs at minutes `00, 10, 20, 30, 40, 50` during hours `00, 04, 08, 12, 16, 20` in `Asia/Kolkata`. Each successful inbound run triggers outbound, and outbound triggers the producer. `catchup=False` prevents Airflow from replaying missed intervals after VM downtime. Each inbound and outbound DAG run currently performs **30 API simulation calls**, so each hourly window can execute up to 180 calls per flow; confirm the workload finishes within the VM window.

1. Create or select an Ubuntu GCE VM in `asia-south1-c` with enough memory for Airflow (at least 8 GB recommended) and a persistent boot disk. Create or select a dedicated GCS service account with only the required bucket access for the mounted key. The existing simulator database must allow connections from the VM. Prefer a private IP for the database. If the VM needs a stable public IP for SSH or Airflow UI access, reserve a static IP.
2. Install Docker Engine and the Docker Compose plugin on the VM, clone this repository, and create a private `.env` from `.env.example`. For Ubuntu, the Docker packages can be installed with:

   ```bash
   sudo apt-get update
   sudo apt-get install -y docker.io docker-compose-v2 git
   sudo systemctl enable --now docker
   ```

   Set `AIRFLOW_UID=50000`, generate `FERNET_KEY` with `python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`, set a random `AIRFLOW__API_AUTH__JWT_SECRET` and `_AIRFLOW_WWW_USER_PASSWORD`, then fill in `SIMULATOR_DB_*` and `KAFKA_*`. For the existing VM PostgreSQL, use `SIMULATOR_DB_HOST=host.docker.internal`, `SIMULATOR_DB_PORT=5432`, `SIMULATOR_DB_NAME=supply_chain`, and the existing role and password (for example, `supplychain_app`). Enable the host database at boot with `sudo systemctl enable --now postgresql`, configure it to listen on the Docker host gateway, and add a `pg_hba.conf` rule restricted to the Compose network subnet and the simulator database role. Do not expose PostgreSQL publicly. Keep `.env` and credential files out of Git. Transfer the GCS service-account JSON to the VM securely at `credentials/daemon-supply-chain-dc9d68e4bf2c.json`, then run `sudo chown 50000:0 credentials/daemon-supply-chain-dc9d68e4bf2c.json && sudo chmod 600 credentials/daemon-supply-chain-dc9d68e4bf2c.json` so the Airflow container can read it; grant that identity access to the bucket. Set `GOOGLE_APPLICATION_CREDENTIALS_HOST` if using a different path.
3. Ensure migrations `001_event_execution.sql` and `002_simulation_runs.sql` have been applied to the existing simulator database. Apply them before deployment; `001_event_execution.sql` also performs data cleanup and should not be run automatically against existing data. Confirm `.env` contains the simulator DB username and password, Kafka credentials, and that the GCS service-account file exists. Compose fails early if the simulator DB username or password is missing. From the repository root on the VM, build and start the services with this single command:

   ```bash
   docker compose up -d --build
   ```

   To inspect startup, run:

   ```bash
   docker compose ps
   docker compose logs --tail=100 airflow-init simulator-api airflow-scheduler airflow-dag-processor
   ```

Compose waits for the simulator API health check, which verifies it can authenticate to the existing simulator PostgreSQL database. The `airflow-dag-unpause` one-shot service then unpauses `simulation_inbound`, `simulation_outbound`, `kafka_producer`, and `kafka_consumer` after the scheduler and DAG processor are healthy. Check `docker compose ps` and `docker compose logs airflow-dag-unpause` if startup does not complete. DAGs run on their configured schedules; `docker compose up` does not trigger an immediate simulation. Successful Kafka/GCS tasks also require valid external Kafka credentials, a reachable broker, an existing bucket, and working GCS credentials. Airflow UI is on port `8080`; restrict firewall access to trusted IPs or use an SSH tunnel. The simulator API binds to `127.0.0.1:8000` by default and is available to Airflow over the internal Compose network.

The Spark master UI is available locally on the VM at `http://127.0.0.1:18080`; worker UIs use ports `18081`-`18083`. Access the master UI from your workstation through an SSH tunnel, for example `gcloud compute ssh daemon-vm --zone=asia-south1-c --ssh-flag="-N" --ssh-flag="-L 18080:127.0.0.1:18080"`. Spark workers each default to 1 core and 1 GiB of worker memory; adjust `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` in `.env` to fit the VM. Spark master RPC is reachable to host clients only through `127.0.0.1:17077`; containers on the Compose network should use `spark://spark-master:7077`. The Spark UIs and RPC port are bound to loopback by default and should not be exposed publicly.
4. To keep the VM on for at most 70 scheduled minutes, start it at 00:00, 04:00, etc. and stop it at 01:10, 05:10, etc. in India time:

   ```bash
   gcloud compute resource-policies create instance-schedule supply-chain-four-hour-window \
     --region=asia-south1 \
     --vm-start-schedule="0 0,4,8,12,16,20 * * *" \
     --vm-stop-schedule="10 1,5,9,13,17,21 * * *" \
     --timezone="Asia/Kolkata"

   gcloud compute instances add-resource-policies daemon-vm \
     --zone=asia-south1-c \
     --resource-policies=supply-chain-four-hour-window
   ```

   Replace `daemon-vm` if the instance has another name. GCE requires the instance schedule policy to be in the VM's region and the Compute Engine service agent to have permission to start and stop instances. GCE can delay scheduled operations by up to 15 minutes; with this strict 70-minute cap, VM and Airflow startup consume part of the DAG window, so some early scheduled runs can be missed. Because `catchup=False`, Airflow won't replay those missed intervals. Review the [GCE instance schedule limitations](https://cloud.google.com/compute/docs/instances/schedule-instance-start-stop#limitations) before attaching it.

Docker's restart policy brings the Compose services back when the VM boots. Verify scheduled runs in the Airflow UI and inspect `docker compose logs` after the first window before relying on unattended operation. Rotating database/Kafka credentials that were previously placed directly in DAG source is also recommended.