# Crypto Deltalake

Pet project to get familiar with websockets, polars, and deltalake.
Getting data from websockets and store them into deltalake.

# How to run

```sh
uv run crypto_deltalake/main.py
```

# Read inserted data

```sh
uv run crypto_deltalake/read.py
```
# Check info of the tables dir

```sh
./bin/check_tables.sh
```

# Run in jupyter
```bash
   uv add ipykernel --dev
   python -m ipykernel install --user --name=crypto_deltalake --display-name=crypto_deltalake
```

Expected output

```sh
Installed kernelspec crypto_deltalake in /home/vlad/.local/share/jupyter/kernels/crypto_deltalake
```

