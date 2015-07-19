# KadiraDB - core

KadiraDB is a low level database for storing time series data.

## Notes

KDB uses memory mapping to increase write performance therefore for KDB to work the `IPC_LOCK` linux capability must be enabled when running inside docker. This can be done easily by adding `--cap-add=IPC_LOCK` when starting the container. Checkout KMDB for an example.
