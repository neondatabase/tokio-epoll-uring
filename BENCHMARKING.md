The `./benchmark` crate is a microbenchmark that evaluates this crate against other solutions.

How to use it:

## Get an `i4i.2xlarge` AWS EC2 instance

Numbers mentioned in the crate docs are for this instance type.

## Build the benchmark binary and upload it

In `.cargo/config.toml`:

```
[target.x86_64-unknown-linux-musl]
linker = "x86_64-linux-gnu-gcc"
```

Then build static binary:

```
cargo build --release --target x86_64-unknown-linux-musl
```

And upload benchmark binary + scripts

```
scp -C -p target/x86_64-unknown-linux-musl/release/benchmark neon-loadtest:/tmp/ && \
    scp -p ./benchmark/scripts/postprocess.py neon-loadtest:/tmp && \
    scp -p ./benchmark/scripts/runbench.sh neon-loadtest:/tmp/ &&
    echo DONE
```

## Prepare Storage on instance

```
sudo mkdir /instance_store && \
    sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 /dev/nvme1n1 && \
    sudo mount /dev/nvme1n1 /instance_store && \
    sudo chown "$(id -u):$(id -g)" /instance_store && \
    echo "DONE"
```

## Run benchmarks

```
cd /instance_store
rm -f *.json *.csv *.tar
bash -x /tmp/runbench.sh
python3 /tmp/postprocess.py
tar -cf results.tar *.json *.csv
```

The results are in the `.csv` files produced by `postprocess.py`.
The most interesting one is the `totals.csv`.

## Viz / Summary

Copy this Google Sheet and import the data.

https://docs.google.com/spreadsheets/d/1bs_q7IyoTzF43SeEIBWCJk7z2mdZZVsLbqvVwE88YWA/edit?usp=sharing

```
# Totals are small enough to fit in RAM
ssh neon-devvm-mbp ssh neon-loadtest cat /instance_store/totals.csv | pbcopy
# per-client stats need to go through CSV import
ssh neon-devvm-mbp ssh neon-loadtest cat /instance_store/fairness_by_nclients-1200.csv > ~/tmp/data.csv
ssh neon-devvm-mbp ssh neon-loadtest cat /instance_store/fairness_by_nclients-400.csv > ~/tmp/data.csv
```

For totals, paste into leftmost column, then "Data => Split Text To Columns".

For CSV, move to the right "fairness" sheet in the bottom bar, click File->Import->Upload csv, select "Replace current sheet".
NB: Not "replace current *spread*sheet", just "replace current sheet".

## Archive data

```
ssh neon-devvm-mbp ssh neon-loadtest cat /instance_store/results.tar > ~/tmp/results.tar
```
