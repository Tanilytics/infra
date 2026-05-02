{{/*
Common helper functions for ingestion chart
*/}}
{{- define "ingestion.labels" -}}
app.kubernetes.io/name: {{ .Values.appName | default "ingestion" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end }}
