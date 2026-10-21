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

## Independent Simulator Runs

Apply `migrations/002_simulation_runs.sql` to PostgreSQL before starting the API. Each independent endpoint can then be called without a request body; PostgreSQL supplies the simulation ID, correlation ID, and default timestamp, and stores run status and results in `simulation_runs` instead of creating files under `output/simulation_runs`.

Each request runs one lifecycle cycle. A scheduler can call the inbound endpoint during the 00:00-12:00 window and the outbound endpoint during the 06:00-18:00 window, up to 120-150 times per window. The API serializes cycles against the shared simulator database; scheduling and cycle counts remain the caller's responsibility.

Run each lifecycle independently from the repository root:

Airflow mounts the `simulator` package so its DAGs can import it. For Google Cloud Storage access, the Compose setup mounts a service-account JSON key read-only from `credentials/daemon-supply-chain-dc9d68e4bf2c.json`; set `GOOGLE_APPLICATION_CREDENTIALS_HOST` in `.env` if the key is stored elsewhere. Keep the key out of version control and grant its service account only the required bucket access.

For local Compose runs, configure the simulator database using `SIMULATOR_DB_HOST`, `SIMULATOR_DB_PORT`, `SIMULATOR_DB_NAME`, `SIMULATOR_DB_USER`, and `SIMULATOR_DB_PASSWORD` in `.env`. Inside Docker, `localhost` is the container itself; use `host.docker.internal` for a database on the Docker host, or the reachable private address for a remote database. The producer opens its database connection only when its task runs.

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

This deployment assumes the existing simulator PostgreSQL database is reachable from the VM. The Compose stack runs the simulator API, Airflow, Airflow's metadata PostgreSQL database, and Redis. Confluent Cloud and Google Cloud Storage remain external services; Spark and BigQuery integrations are not implemented in this repository.

The inbound DAG runs at minutes `00, 10, 20, 30, 40, 50` during hours `00, 04, 08, 12, 16, 20` in `Asia/Kolkata`. Each successful inbound run triggers outbound, and outbound triggers the producer. `catchup=False` prevents Airflow from replaying missed intervals after VM downtime. Each inbound and outbound DAG run currently performs **30 API simulation calls**, so each hourly window can execute up to 180 calls per flow; confirm the workload finishes within the VM window.

1. Create or select an Ubuntu GCE VM in `asia-south1-c` with enough memory for Airflow (at least 8 GB recommended) and a persistent boot disk. Create or select a dedicated GCS service account with only the required bucket access for the mounted key. The existing simulator database must allow connections from the VM. Prefer a private IP for the database. If the VM needs a stable public IP for SSH or Airflow UI access, reserve a static IP.
2. Install Docker Engine and the Docker Compose plugin on the VM, clone this repository, and create a private `.env` from `.env.example`. For Ubuntu, the Docker packages can be installed with:

   ```bash
   sudo apt-get update
   sudo apt-get install -y docker.io docker-compose-v2 git
   sudo systemctl enable --now docker
   ```

   Set `AIRFLOW_UID=50000`, generate `FERNET_KEY` with `python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`, set a random `AIRFLOW__API_AUTH__JWT_SECRET` and `_AIRFLOW_WWW_USER_PASSWORD`, then fill in the `SIMULATOR_DB_*` and `KAFKA_*` values. The simulator DB host must be reachable from the VM (use `host.docker.internal` if PostgreSQL runs on the same VM); port `5432` is the default. Keep `.env` and credential files out of Git. Transfer the GCS service-account JSON to the VM securely at `credentials/daemon-supply-chain-dc9d68e4bf2c.json`, then run `sudo chown 50000:0 credentials/daemon-supply-chain-dc9d68e4bf2c.json && sudo chmod 600 credentials/daemon-supply-chain-dc9d68e4bf2c.json` so the Airflow container can read it; grant that identity access to the bucket. Set `GOOGLE_APPLICATION_CREDENTIALS_HOST` if using a different path.
3. Ensure migrations `001_event_execution.sql` and `002_simulation_runs.sql` have been applied to the simulator database. From the repository root on the VM, build and start the services:

   ```bash
   docker compose up -d --build
   docker compose ps
   docker compose logs --tail=100 airflow-init simulator-api airflow-scheduler airflow-dag-processor
   docker compose exec airflow-scheduler airflow dags unpause simulation_inbound
   docker compose exec airflow-scheduler airflow dags unpause simulation_outbound
   docker compose exec airflow-scheduler airflow dags unpause kafka_producer
   docker compose exec airflow-scheduler airflow dags unpause kafka_consumer
   ```

   Airflow UI is on port `8080`; restrict firewall access to trusted IPs or use an SSH tunnel. The simulator API binds to `127.0.0.1:8000` by default and is available to Airflow over the internal Compose network.
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