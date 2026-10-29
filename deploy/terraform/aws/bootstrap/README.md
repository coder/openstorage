# AWS Instance Bootstrap

## Overview

This directory holds the files that cloud-init copies to the EC2 instance of the AWS reference deployment. Terraform embeds every file listed in `compute.tf` (`bootstrap.py`, `runtime.py`, and the three systemd units) into the instance user data. Cloud-init writes them to `/etc/openstorage/` with mode `0600` and then runs `/usr/bin/python3 /etc/openstorage/bootstrap.py` once [../compute.tf:local.cloud_config].

- `bootstrap.py` runs once as root. It installs the pinned release binary, the RDS CA bundle, and the systemd units, then starts the service.
- `runtime.py` runs as root before every service start (`prepare`) and once a minute from a timer (`refresh`). It fetches the RDS credentials from Secrets Manager and writes the root-only systemd environment file `/run/openstorage/service.env`.
- `openstorage.service` runs `openstorage serve` as the unprivileged `openstorage` user.
- `Dockerfile.test` and `container_probe.py` are a test harness only. They do not reach the instance.

## Interface

### `bootstrap.py` (cloud-init entry point, no arguments)

- `UNITS`: the three unit file names `openstorage.service`, `openstorage-refresh.service`, `openstorage-refresh.timer` [bootstrap.py:UNITS].
- `CA_URL`: `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` [bootstrap.py:CA_URL].
- `validate_public_key(path)`: accepts one ASCII PEM `PUBLIC KEY` block and checks it with `openssl pkey -pubin -pubcheck -noout` [bootstrap.py:validate_public_key].
- `install_release(archive_path, expected_sha256, tag, destination)`: verifies and installs the release binary. Returns the `LICENSE` bytes or `None` [bootstrap.py:install_release].
- `NoRedirect`: a `urllib.request.HTTPRedirectHandler` that raises on any redirect [bootstrap.py:NoRedirect].
- `install_ca(destination=CA_FILE)`: downloads and installs the RDS CA bundle [bootstrap.py:install_ca].
- `bootstrap()`: the full install flow [bootstrap.py:bootstrap].
- `main()`: returns `0`, or `1` with the fixed stderr message `openstorage bootstrap failed (details withheld)` [bootstrap.py:main].

### `runtime.py` (CLI: `runtime.py prepare` or `runtime.py refresh`)

- `CONFIG_DIR = /etc/openstorage`, `RUN_DIR = /run/openstorage`, `CA_FILE = /etc/openstorage/rds-ca.pem` [runtime.py:CONFIG_DIR] [runtime.py:RUN_DIR] [runtime.py:CA_FILE].
- `parse_json(data)`: `json.loads` that rejects duplicate object keys [runtime.py:parse_json].
- `validate_config(cfg)`: strict check of `config.json`. The key set must be exactly `region`, `account_id`, `org`, `service_url`, `artifact_bucket`, `artifact_key`, `release_sha256`, `release_tag`, `postgres_host`, `postgres_port`, `postgres_database`, `postgres_secret_arn`, `data_bucket`, `trusted_proxy_cidrs` [runtime.py:validate_config].
- `load_config()`: reads `/etc/openstorage/config.json` through `parse_json` and `validate_config` [runtime.py:load_config].
- `run(args, timeout=45)`: subprocess wrapper. Returns stdout bytes or raises `RuntimeError("subprocess failed (output withheld)")` [runtime.py:run].
- `atomic_write(path, content, mode=0o600)`: write to a temporary file in the same directory, `fchmod`, `fsync`, then `os.replace` [runtime.py:atomic_write].
- `environment(cfg, response)`: builds the systemd `EnvironmentFile` content from the config and the `get-secret-value` JSON response [runtime.py:environment].
- `refresh(cfg, directory=RUN_DIR, restart=False)`: returns `True` when `service.env` changed, `False` when it is unchanged [runtime.py:refresh].
- `main(args=None)`: `prepare` calls `refresh(restart=False)`. `refresh` calls `refresh(restart=True)` [runtime.py:main].

### `runtime.py environment` output (variables passed to `openstorage serve`)

`AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_EC2_METADATA_DISABLED`, `OPENSTORAGE_ORG`, `OPENSTORAGE_ORG_PUBKEYS`, `OPENSTORAGE_BASE_URL`, `OPENSTORAGE_LISTEN=0.0.0.0:8080`, `OPENSTORAGE_PG_URL`, `OPENSTORAGE_S3_URL`, `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`, `OPENSTORAGE_RATE_RPS=20`, `OPENSTORAGE_RATE_BURST=40`, `OPENSTORAGE_TRUSTED_PROXIES`, `OPENSTORAGE_SYNC_ALLOW_PRIVATE=false`, `OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE=false`, `TMPDIR=/var/lib/openstorage/tmp` [runtime.py:environment].

### systemd units

| Unit | Runs | Key settings |
|---|---|---|
| `openstorage.service` | `ExecStartPre=+/usr/bin/python3 /etc/openstorage/runtime.py prepare` (root), then `ExecStart=/usr/local/bin/openstorage serve --cache-dir=/var/cache/openstorage` as `User=openstorage` | `EnvironmentFile=-/run/openstorage/service.env`, `Restart=on-failure`, `KillMode=mixed`, `TimeoutStopSec=30s`, `UMask=0077`, `NoNewPrivileges=true`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp=true`, empty `CapabilityBoundingSet=`, `UnsetEnvironment=` of static AWS credential and endpoint variables [openstorage.service:Service] |
| `openstorage-refresh.service` | `Type=oneshot`, `ExecStart=/usr/bin/python3 /etc/openstorage/runtime.py refresh` as root | `UMask=0077`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectHome=true` [openstorage-refresh.service:Service] |
| `openstorage-refresh.timer` | `Unit=openstorage-refresh.service` | `OnBootSec=1min`, `OnUnitInactiveSec=1min`, `AccuracySec=1s` [openstorage-refresh.timer:Timer] |

### Test harness (not installed on the instance)

- `Dockerfile.test`: an `amazonlinux:2023` image pinned by digest. Its `CMD` runs `python3 -m unittest discover -v && python3 container_probe.py` [Dockerfile.test].
- `ContainerProbe.test_bootstrap_and_runtime`: a destructive end-to-end probe of `bootstrap.bootstrap()` and `runtime.refresh()` with `aws` and `systemctl` stubbed. `setUpClass` refuses to run unless the process is root inside a Docker container on Amazon Linux [container_probe.py:ContainerProbe].

## Invariants

- `bootstrap()` refuses to run unless the effective UID is 0 [bootstrap.py:bootstrap].
- `bootstrap()` refuses any host other than Amazon Linux 2023 (`ID=amzn`, `VERSION_ID=2023`) on `x86_64` [bootstrap.py:bootstrap].
- The `openstorage` account must not have UID 0 or GID 0 [bootstrap.py:bootstrap].
- `install_release` refuses an archive whose SHA-256 differs from `release_sha256`. This is a checksum check only. No signature is verified [bootstrap.py:install_release].
- `install_release` accepts only the member sets `{openstorage}` or `{openstorage, LICENSE}`. Each member must be a regular, non-sparse file of at most 512 MiB. It never calls `extract` or `extractall`, so archive paths, owners, and modes are ignored [bootstrap.py:install_release].
- `install_release` runs the extracted binary with `--version`. The output must be one line that starts with `openstorage <release_tag> (` [bootstrap.py:install_release].
- The RDS CA download refuses redirects and responses over 2 MiB. The content must load as an SSL trust store before it is written [bootstrap.py:NoRedirect] [bootstrap.py:install_ca].
- `bootstrap()` changes the mode and owner of `/var/cache/openstorage`, `/var/lib/openstorage`, and `/var/lib/openstorage/tmp` through an `O_NOFOLLOW` descriptor. A symlink planted at one of those paths makes it fail [bootstrap.py:bootstrap].
- The S3 download uses `aws s3api get-object --expected-bucket-owner <account_id>` [bootstrap.py:bootstrap].
- `validate_config` requires the exact key set, a regex match on each string field, `postgres_port` of type `int` equal to `5432`, and `postgres_database` equal to `openstorage` [runtime.py:validate_config].
- `service_url` must be an HTTPS origin: a host, and no user info, path other than `/`, query, or fragment [runtime.py:validate_config].
- `postgres_secret_arn` must start with the Secrets Manager ARN prefix of the configured region and account [runtime.py:validate_config].
- `trusted_proxy_cidrs` must be a non-empty list of CIDRs with a prefix length above 0 [runtime.py:validate_config].
- `run` never inherits the caller's environment. It sets a fixed `PATH`, `HOME=/nonexistent`, and `AWS_CONFIG_FILE=/dev/null` and `AWS_SHARED_CREDENTIALS_FILE=/dev/null`. It never puts subprocess output in errors [runtime.py:run].
- `environment` requires a string `SecretString` and a `VersionId` matching `[A-Za-z0-9-]{32,64}`. The username and password must be non-empty strings without NUL [runtime.py:environment].
- The database DSN uses `sslmode=verify-full` with `sslrootcert=/etc/openstorage/rds-ca.pem`. The credentials are percent-encoded into the file and never appear in argv [runtime.py:environment].
- `environment` asserts that no value contains a control character, and it escapes `\` and `"` for systemd `EnvironmentFile` syntax [runtime.py:environment].
- `refresh` refuses a `/run/openstorage` that is not a directory owned by the current user. It refuses an existing `service.env` that is not a regular file owned by the current user with mode `0600` [runtime.py:refresh].
- `refresh` holds an exclusive `flock` on `/run/openstorage/refresh.lock` for the whole update [runtime.py:refresh].
- `refresh` writes nothing when the new content equals the old content [runtime.py:refresh].
- `refresh` enqueues the restart with `systemctl --no-block`, because `ExecStartPre` takes the same lock. If the restart cannot be enqueued, it restores the previous `service.env` (or deletes the new one) and re-raises [runtime.py:refresh].
- `runtime.py main` refuses to run unless it is root and the arguments are exactly `prepare` or `refresh`. On failure both entry points print a fixed message with no exception details [runtime.py:main] [bootstrap.py:main].

## Side Effects

- `bootstrap()` runs `dnf install -y --allowerasing git gnupg2 openssh-clients python3 awscli-2 openssl shadow-utils ca-certificates` [bootstrap.py:bootstrap].
- `bootstrap()` creates the system user `openstorage` with `useradd --system --user-group --home-dir /var/lib/openstorage --shell /sbin/nologin` when it is missing [bootstrap.py:bootstrap].
- `bootstrap()` sets `/etc/openstorage` to `0755`, `config.json` to `0600`, and `org.pub.pem` to `0644` after validation [bootstrap.py:bootstrap].
- `bootstrap()` creates `/var/cache/openstorage`, `/var/lib/openstorage`, and `/var/lib/openstorage/tmp` with mode `0700`, owned by `openstorage` [bootstrap.py:bootstrap].
- `bootstrap()` downloads the release archive from S3 with `aws s3api get-object` into a temporary directory [bootstrap.py:bootstrap].
- `install_release` replaces `/usr/local/bin/openstorage` (mode `0755`). `bootstrap()` writes `/etc/openstorage/LICENSE` (`0644`) when the archive has one [bootstrap.py:install_release] [bootstrap.py:bootstrap].
- `install_ca` makes an HTTPS request to `CA_URL` and writes `/etc/openstorage/rds-ca.pem` (`0644`) [bootstrap.py:install_ca].
- `bootstrap()` copies the three units to `/etc/systemd/system/` (`0644`), runs `systemctl daemon-reload`, and runs `systemctl enable --now openstorage.service openstorage-refresh.timer` [bootstrap.py:bootstrap].
- `refresh` calls `aws secretsmanager get-secret-value --version-stage AWSCURRENT` for `postgres_secret_arn` [runtime.py:refresh].
- `refresh` writes `/run/openstorage/service.env` (`0600`, root) and creates `/run/openstorage/refresh.lock` [runtime.py:refresh].
- `refresh` with `restart=True` runs `systemctl --no-block try-restart openstorage.service` when the credentials changed [runtime.py:refresh].
- The timer starts the refresh service one minute after boot and one minute after each run ends [openstorage-refresh.timer:Timer].
- `container_probe.py` overwrites system paths (`/etc/openstorage`, `/var/lib/openstorage`, `/run/openstorage`, `/etc/systemd/system`). Run it only inside the `Dockerfile.test` container [container_probe.py:ContainerProbe].

## Verification

Run these from the repository root. CI runs the same commands in the `terraform` job of `.github/workflows/ci.yml`.

```sh
python3 -m unittest discover -s deploy/terraform/aws/bootstrap -p 'test_*.py' -v
docker build -t openstorage-bootstrap:test -f deploy/terraform/aws/bootstrap/Dockerfile.test deploy/terraform/aws/bootstrap
docker run --rm --network=none openstorage-bootstrap:test
```

Related docs: [Deploy on AWS with Terraform](../../../../docs/how-to/deploy-aws-terraform.md).
