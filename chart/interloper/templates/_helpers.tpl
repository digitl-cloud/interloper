{{/*
Fully-qualified name (defaults to <release>-<chart>, truncated to 63 chars).
*/}}
{{- define "interloper.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "interloper.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to all resources.
*/}}
{{- define "interloper.labels" -}}
helm.sh/chart: {{ include "interloper.chart" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Selector labels for a specific component.
Usage: {{ include "interloper.selectorLabels" (dict "root" . "component" "scheduler") }}
*/}}
{{- define "interloper.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "interloper.componentLabels" -}}
{{ include "interloper.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
ServiceAccount name — defaults to <release>-scheduler.
*/}}
{{- define "interloper.serviceAccountName" -}}
{{- default (printf "%s-scheduler" (include "interloper.fullname" .)) .Values.serviceAccount.name -}}
{{- end -}}

{{/*
Secret name — either user-provided existingSecret or chart-generated.
*/}}
{{- define "interloper.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{ .Values.secrets.existingSecret }}
{{- else -}}
{{ include "interloper.fullname" . }}
{{- end -}}
{{- end -}}

{{/*
Flavor token for the scheduler image, derived from the runtime launcher
choice (config.launcher.type). Flavors are tag suffixes on the single
interloper-scheduler image, matching the build-time SCHEDULER_EXTRAS that
produced each variant.

  in_process → ""        (base image, no launcher extras)
  kubernetes → "k8s"
  docker     → "docker"
*/}}
{{- define "interloper.schedulerFlavor" -}}
{{- $type := .Values.config.launcher.type | default "in_process" -}}
{{- if eq $type "kubernetes" -}}k8s
{{- else if eq $type "docker" -}}docker
{{- end -}}
{{- end -}}

{{/*
Flavor token for the api image. The "agent" flavor
(interloper-api:<tag>-agent) bundles interloper-agent so the /agent routes
mount; the base image omits it (those routes 404).
*/}}
{{- define "interloper.apiFlavor" -}}
{{- if .Values.api.agent.enabled -}}agent{{- end -}}
{{- end -}}

{{/*
Image reference for a component. Falls back to
"<registry>/interloper-<component>:<tag>[-<flavor>]" if repository is not set.
The optional "flavor" rides the tag (e.g. scheduler "-k8s", api "-agent").
Usage: {{ include "interloper.image" (dict "root" . "component" "scheduler" "image" .Values.scheduler.image "flavor" (include "interloper.schedulerFlavor" .)) }}
*/}}
{{- define "interloper.image" -}}
{{- $tag := default .root.Values.image.tag .image.tag | default .root.Chart.AppVersion -}}
{{- $flavor := .flavor | default "" -}}
{{- $suffix := "" -}}
{{- if $flavor -}}{{- $suffix = printf "-%s" $flavor -}}{{- end -}}
{{- if .image.repository -}}
{{ .image.repository }}:{{ $tag }}{{ $suffix }}
{{- else -}}
{{ .root.Values.image.registry }}/interloper-{{ .component }}:{{ $tag }}{{ $suffix }}
{{- end -}}
{{- end -}}

{{/*
Scheduler image reference (used as default for the K8s launcher).
*/}}
{{- define "interloper.schedulerImage" -}}
{{ include "interloper.image" (dict "root" . "component" "scheduler" "image" .Values.scheduler.image "flavor" (include "interloper.schedulerFlavor" .)) }}
{{- end -}}

{{/*
Worker image reference (used as default for the K8s runner — leaf
per-asset Jobs). No launcher suffix: the worker doesn't spawn sub-Jobs.
*/}}
{{- define "interloper.workerImage" -}}
{{- $tag := default .Values.image.tag .Values.worker.image.tag | default .Chart.AppVersion -}}
{{- if .Values.worker.image.repository -}}
{{ .Values.worker.image.repository }}:{{ $tag }}
{{- else -}}
{{ .Values.image.registry }}/interloper-worker:{{ $tag }}
{{- end -}}
{{- end -}}

{{/*
Postgres host: either the bundled subchart service or external config.
*/}}
{{- define "interloper.postgresHost" -}}
{{- if .Values.postgresql.enabled -}}
{{ .Release.Name }}-postgresql
{{- else -}}
{{ required "externalPostgres.host is required when postgresql.enabled=false" .Values.externalPostgres.host }}
{{- end -}}
{{- end -}}

{{- define "interloper.postgresPort" -}}
{{- if .Values.postgresql.enabled -}}
5432
{{- else -}}
{{ .Values.externalPostgres.port | default 5432 }}
{{- end -}}
{{- end -}}

{{- define "interloper.postgresUser" -}}
{{- if .Values.postgresql.enabled -}}
{{ .Values.postgresql.auth.username }}
{{- else -}}
{{ .Values.externalPostgres.user }}
{{- end -}}
{{- end -}}

{{- define "interloper.postgresDatabase" -}}
{{- if .Values.postgresql.enabled -}}
{{ .Values.postgresql.auth.database }}
{{- else -}}
{{ .Values.externalPostgres.database }}
{{- end -}}
{{- end -}}

{{/*
Environment variables shared by all interloper pods (scheduler, api, frontend).
*/}}
{{- define "interloper.commonEnv" -}}
- name: INTERLOPER_POSTGRES_HOST
  value: {{ include "interloper.postgresHost" . | quote }}
- name: INTERLOPER_POSTGRES_PORT
  value: {{ include "interloper.postgresPort" . | quote }}
- name: INTERLOPER_POSTGRES_USER
  value: {{ include "interloper.postgresUser" . | quote }}
- name: INTERLOPER_POSTGRES_DATABASE
  value: {{ include "interloper.postgresDatabase" . | quote }}
- name: INTERLOPER_POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "interloper.secretName" . }}
      key: INTERLOPER_POSTGRES_PASSWORD
      optional: true
- name: INTERLOPER_ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "interloper.secretName" . }}
      key: INTERLOPER_ENCRYPTION_KEY
      optional: true
- name: INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "interloper.secretName" . }}
      key: INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET
      optional: true
{{- end -}}
