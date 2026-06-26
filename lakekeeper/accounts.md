Man kann das wohl nach Kubernetes Service accounts machen:
# Beispielhaft in den Umgebungsvariablen deines Lakekeeper Helm Charts
extraEnv:
  - name: LAKEKEEPER__INSTANCE_ADMINS
    value: '["kubernetes~system:serviceaccount:data-layer:clickhouse-sa"]'
                                                namepsace:service

Problem ist aber, dass zB bei QueryEngines in der Regel das fixe Token mitgegeben werden muss
    -> muss dann jede Stunde oder so neu gemacht werden.
Lösen könnte man das über ein Sidecar, aber das müsste man dann auch für jede Engine neu schreiben...
