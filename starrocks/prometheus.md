# StarRocks Metrics & Prometheus

## Endpoints

| Komponente | URL | Port |
|---|---|---|
| FE-Service | `/metrics` | 8030 (`http_port`) |
| CN-Service | `/metrics` | 8040 (`webserver_port`) |

Default aktiviert, keine Extra-Config in StarRocks nötig.

## Mit Prometheus-Operator (kube-prometheus-stack)

Nichts an Prometheus anfassen. `values-prod.yaml` erzeugt den `ServiceMonitor`:
```yaml
metrics:
  serviceMonitor:
    enabled: true
    labels:
      release: prometheus   # MUSS zum serviceMonitorSelector der Prometheus-CRD passen
```

Selector pruefen:
```bash
kubectl get prometheus -A -o yaml | grep -A3 serviceMonitorSelector
```

## Ohne Operator (Standalone Prometheus)

`serviceMonitor.enabled: false` setzen und in `prometheus.yml` ergaenzen:

```yaml
scrape_configs:
  - job_name: starrocks
    kubernetes_sd_configs:
      - role: pod
        namespaces: { names: [starrocks] }
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_part_of]
        regex: starrocks
        action: keep
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        target_label: component
      - source_labels: [component, __meta_kubernetes_pod_ip]
        regex: 'fe;(.+)'
        replacement: '${1}:8030'
        target_label: __address__
      - source_labels: [component, __meta_kubernetes_pod_ip]
        regex: 'cn;(.+)'
        replacement: '${1}:8040'
        target_label: __address__
    metrics_path: /metrics
```

**Pushgateway nicht verwenden** -- ist fuer Batch-Jobs, nicht fuer langlebige Services.

## Verifikation

```bash
kubectl get servicemonitor -n starrocks            # Operator-Pfad
kubectl port-forward -n monitoring svc/prometheus-operated 9090:9090
# Browser -> http://localhost:9090/targets -> starrocks-fe / starrocks-cn UP?
```

## Wichtige PromQL

```promql
# Query-Latenz p95
histogram_quantile(0.95,
  sum by (le) (rate(starrocks_fe_query_latency_ms_bucket[5m])))

# Datacache Hit-Rate (Hebel-Indikator fuer MV-Entscheidung)
sum(rate(starrocks_be_datacache_hit_count[5m]))
/ sum(rate(starrocks_be_datacache_access_count[5m]))

# QPS pro FE
sum by (instance) (rate(starrocks_fe_query_total[1m]))

# CN-Memory-Pressure
starrocks_be_process_mem_bytes / starrocks_be_mem_limit_bytes
```

Grafana: offizielles StarRocks-Dashboard via [grafana.com/grafana/dashboards](https://grafana.com/grafana/dashboards/?search=starrocks) importieren.
