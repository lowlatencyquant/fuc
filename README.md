# FUC

A C++20 CLI for Fusion with automatic Keycloak login.

## Build

```sh
cmake --preset debug
cmake --build --preset debug
export FUC_CONFIG="$PWD/config.local.yaml"
```

Set your API URL and Keycloak credentials in `config.local.yaml`, or override them with `FUC_*` environment variables.

## Commands

- `fuc login` — Test your Keycloak credentials.
- `fuc health` — Check API health without authentication.
- `fuc artifacts --name org.data --page-size 20` — Read artifacts with optional filters and pagination.
- `fuc kafka create NAME --chain CHAIN --broker HOST:PORT --topic TOPIC --group GROUP` — Create a Kafka trigger for an existing chain, automatically constructing the JSON and headers.
- `fuc kafka delete NAME` — Delete the named Kafka trigger.

Run `./build/debug/fuc` in place of `fuc` when using the local build.

## Kafka example

```sh
./build/debug/fuc kafka create orders \
  --chain process-orders \
  --broker kafka:9092 \
  --topic orders \
  --group orders-workers \
  --event put \
  --max-concurrent 2
```

Repeat `--broker`, `--event`, or `--bucket` for multiple values; use `--secret NAME` for a Kafka credentials secret.

Add `--dry-run` to preview an API request without sending it, or `--help` to see command options.

## Development

Run `ctest --preset debug` for tests, or open the folder in VS Code and press F5 to build and debug the selected launch configuration.
