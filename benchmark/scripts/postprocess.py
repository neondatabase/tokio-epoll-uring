from pathlib import Path
import json

def iter_outputs_by_name():
    output_suffix = ".output.json"
    for p in Path("./").glob("*" + output_suffix):
        name = p.name[:-len(output_suffix)]
        output = json.load(p.open("r"))
        yield name, output

all_data = [{"name": name, "output": output} for name, output in iter_outputs_by_name()]
all_data.sort(key=lambda x: x["name"])

with open("all_data.json", "w") as f:
    json.dump(all_data, f, indent=4)

desired_totals = [
    (["throughput_iops"], "IOPS"),
    (["throughput_bw_mibps"], "MiB/s"),
    (["latency_min_us"], "min (us)"),
    (["latency_mean_us"], "mean (us)"),
    (["latency_max_us"], "max (us)"),
    (["latency_percentiles", "p50"], "p50 (us)"),
    (["latency_percentiles", "p90"], "p90 (us)"),
    (["latency_percentiles", "p99"], "p99 (us)"),
    (["latency_percentiles", "p99.9"], "p99.9 (us)"),
    (["latency_percentiles", "p99.99"], "p99.99 (us)"),
    (["latency_percentiles", "p99.999"], "p99.999 (us)"),
]
import csv
with open("totals.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["metric"] + [data["name"] for data in all_data])
    for path, display in desired_totals:
        def lookup_json_path(json, path):
            for key in path:
                json = json[key]
            return json
        writer.writerow([display] + [lookup_json_path(data["output"]["totals"][-1], path) for data in all_data])

by_nclients = {}
for data in all_data:
    nclients = data["output"]["args"]["num_clients"]
    by_nclients.setdefault(nclients, []).append(data)
for nclients, data in by_nclients.items():
    with open(f"fairness_by_nclients-{nclients}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["client"] + [data["name"] for data in data])
        for i in range(0, nclients):
            writer.writerow([i] + [data["output"]["sorted_per_task_total_reads"][i] for data in data])

