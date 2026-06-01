{{/*
Common helper functions for processing chart
*/}}
{{- define "processing.labels" -}}
app.kubernetes.io/name: {{ .Values.appName | default "processing" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end }}
