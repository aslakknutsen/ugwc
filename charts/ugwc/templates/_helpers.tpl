{{/*
Expand the name of the chart.
*/}}
{{- define "ugwc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "ugwc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label
*/}}
{{- define "ugwc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ugwc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ugwc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ugwc.labels" -}}
helm.sh/chart: {{ include "ugwc.chart" . }}
{{ include "ugwc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "ugwc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ugwc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
xDS bootstrap for Praxis ISTIOD_ADDR (tonic needs a URI scheme on generated defaults).
*/}}
{{- define "ugwc.bootstrapAddr" -}}
{{- if .Values.ugwc.controlPlaneXdsBootstrapAddr }}
{{- .Values.ugwc.controlPlaneXdsBootstrapAddr }}
{{- else if .Values.ugwc.xdsInsecure }}
{{- printf "http://%s.%s.svc.cluster.local:%v" (include "ugwc.fullname" .) .Release.Namespace .Values.service.ports.xds }}
{{- else }}
{{- printf "https://%s.%s.svc.cluster.local:%v" (include "ugwc.fullname" .) .Release.Namespace .Values.service.ports.xds }}
{{- end }}
{{- end }}
