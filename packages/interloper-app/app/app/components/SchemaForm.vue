<script setup lang="ts">
/**
 * Auto-generates a form from a JSON Schema (as produced by Pydantic's model_json_schema()).
 *
 * Supports `x-widget` extensions for widget hints:
 *   - text (default for string)
 *   - password
 *   - textarea
 *   - number (default for integer/number)
 *   - switch (default for boolean)
 *   - select (default for enum)
 *
 * Supports `x-oauth` at the schema root for OAuth sign-in:
 *   - UTabs toggle between "Sign in" and "Manual" modes
 *   - Sign-in tab shows the OAuth button, manual tab shows all credential fields
 *
 * When no x-widget is specified, the widget is inferred from the JSON Schema type.
 */

import type { TabsItem } from '@nuxt/ui'

interface FetchMeta {
    /** `<slot>.<method>` resolved via `/external/resolve`. */
    provider: string
    label_key: string
    value_key: string
}

interface JsonSchemaProperty {
    type?: string
    title?: string
    description?: string
    default?: unknown
    enum?: unknown[]
    'x-widget'?: string
    'x-options'?: { label: string, value: string }[]
    'x-options-from'?: string
    'x-fetch'?: FetchMeta
    /** Field resolved by the OAuth flow — hidden in sign-in mode. */
    'x-oauth-managed'?: boolean
    minimum?: number
    maximum?: number
    minLength?: number
    maxLength?: number
}

interface JsonSchema {
    properties?: Record<string, JsonSchemaProperty>
    required?: string[]
    'x-oauth'?: OAuthFieldMeta
}

const props = defineProps<{
    schema: JsonSchema
    /**
     * Catalog key of the component this schema belongs to (e.g. the source key).
     * Required for provider-backed `x-fetch` fields, which resolve via
     * `/external/resolve` using this key.
     */
    componentKey?: string
    /** Fields to exclude from the form (e.g. 'id'). */
    exclude?: string[]
    /**
     * Resource data from sibling steps, keyed by slot name.
     * Used by `x-fetch` fields, which resolve options from the resource in
     * their provider's slot.
     * Each value is the resource's stored `data` object (credentials, config, etc.).
     * Takes precedence over `resourceIds` for the same slot.
     */
    resourceContext?: Record<string, Record<string, unknown>>
    /**
     * Resource IDs keyed by slot name. SchemaForm fetches the resource
     * detail internally to resolve `x-fetch` dependencies.
     * Used in edit mode where only IDs are known.
     */
    resourceIds?: Record<string, string>
    /**
     * Dynamic options for `x-options-from` fields, keyed by entity name
     * (e.g. `"destinations"`). Each value is a list of `{label, value}` options.
     */
    optionsContext?: Record<string, { label: string; value: string }[]>
}>()

const data = defineModel<Record<string, any>>('data', { default: () => ({}) })
const isValid = defineModel<boolean>('isValid', { default: false })

const catalogStore = useCatalogStore()

/** Tracks which password fields have their value visible. */
const revealedFields = ref<Record<string, boolean>>({})

/** Active tab: 'oauth' or 'manual'. */
const activeTab = ref<string | number>('oauth')

/** Detect the OAuth config from the schema root. */
const oauthMeta = computed<OAuthFieldMeta | null>(() => {
    return props.schema?.['x-oauth'] ?? null
})

/** Whether the OAuth provider is available on the server. */
const oauthAvailable = computed(() => {
    if (!oauthMeta.value) return false
    return catalogStore.isOAuthProviderAvailable(oauthMeta.value.provider)
})

/** Provider display label. */
const oauthLabel = computed(() => {
    if (!oauthMeta.value) return 'OAuth'
    return oauthMeta.value.label || oauthMeta.value.provider
})

/** Tabs items for OAuth toggle. */
const oauthTabs = computed<TabsItem[]>(() => [
    { label: `Sign in with ${oauthLabel.value}`, value: 'oauth', icon: 'i-lucide-lock-open' },
    { label: 'Manual', value: 'manual', icon: 'i-lucide-keyboard' },
])

/** Field keys auto-filled by the OAuth token exchange (used for the fill check). */
const oauthTokenFieldKeys = computed<Set<string>>(() => {
    if (!oauthMeta.value) return new Set()
    return new Set(Object.values(oauthMeta.value.fields))
})

/**
 * Field keys hidden in sign-in mode: everything the OAuth flow resolves for
 * the user — token-filled fields plus app credentials from env (e.g. client_id
 * / client_secret). The backend tags these with `x-oauth-managed`; falls back
 * to the token fields if none are tagged.
 */
const oauthFieldKeys = computed<Set<string>>(() => {
    if (!oauthMeta.value) return new Set()
    const tagged = Object.entries(props.schema?.properties ?? {})
        .filter(([, prop]) => prop['x-oauth-managed'])
        .map(([key]) => key)
    return new Set(tagged.length ? tagged : [...oauthTokenFieldKeys.value])
})

/** Whether OAuth fields have been filled (sign-in completed). */
const oauthFilled = computed(() => {
    if (!oauthMeta.value) return false
    return [...oauthTokenFieldKeys.value].every(
        key => data.value[key] !== undefined && data.value[key] !== '',
    )
})

// ── Resource resolution ──────────────────────────────────────────
const resourcesStore = useResourcesStore()

/** Internally-fetched resource data from `resourceIds` prop. */
const resolvedResources = ref<Record<string, Record<string, unknown>>>({})

/** Fetch resource details for any `resourceIds` entries. */
watch(
    () => props.resourceIds,
    async (ids) => {
        if (!ids) return
        for (const [slotName, resourceId] of Object.entries(ids)) {
            if (!resourceId) continue
            // Skip if already provided via resourceContext
            if (props.resourceContext?.[slotName]) continue
            // Skip if already resolved for this ID
            if ((resolvedResources.value[slotName] as any)?._id === resourceId) continue
            try {
                const detail = await resourcesStore.fetchOne(resourceId)
                resolvedResources.value = {
                    ...resolvedResources.value,
                    [slotName]: { ...detail.data, _id: resourceId },
                }
            }
            catch {
                // Don't block the form if a resource can't be fetched
            }
        }
    },
    { immediate: true, deep: true },
)

/**
 * Merged resource context: explicit `resourceContext` prop takes
 * precedence, then internally resolved data from `resourceIds`.
 */
const mergedResourceContext = computed<Record<string, Record<string, unknown>>>(() => {
    const ctx: Record<string, Record<string, unknown>> = {}
    // Layer 1: internally resolved (strip _id marker)
    for (const [slotName, data] of Object.entries(resolvedResources.value)) {
        const { _id, ...rest } = data
        ctx[slotName] = rest
    }
    // Layer 2: explicit resourceContext overrides
    if (props.resourceContext) {
        Object.assign(ctx, props.resourceContext)
    }
    return ctx
})

// ── Fetch fields ─────────────────────────────────────────────────
const { apiFetch } = useApi()

/** Per-field fetch state: options, loading, error. */
const fetchState = ref<Record<string, {
    options: { label: string, value: string }[]
    loading: boolean
    error: string | null
}>>({})

/** Ensure a fetch state entry exists for a field key. */
function ensureFetchState(fieldKey: string) {
    if (!fetchState.value[fieldKey]) {
        fetchState.value = {
            ...fetchState.value,
            [fieldKey]: { options: [], loading: false, error: null },
        }
    }
}

/** Update a single fetch state entry reactively. */
function updateFetchState(fieldKey: string, patch: Partial<{ options: { label: string, value: string }[], loading: boolean, error: string | null }>) {
    fetchState.value = {
        ...fetchState.value,
        [fieldKey]: { ...fetchState.value[fieldKey]!, ...patch },
    }
}

/** The resource slot a fetch field depends on — the provider's `<slot>`. */
function fetchSlot(meta: FetchMeta): string {
    return meta.provider.split('.')[0] ?? ''
}

/** Fetch options for a single x-fetch field. */
async function fetchOptions(fieldKey: string, meta: FetchMeta) {
    ensureFetchState(fieldKey)

    // Backend instantiates the resource in the provider's slot and calls the
    // provider method. Credentials are sent keyed by slot.
    const slot = fetchSlot(meta)
    const deps: Record<string, unknown> = {}
    const resourceData = mergedResourceContext.value[slot]
    if (resourceData) deps[slot] = resourceData
    else if (data.value[slot] !== undefined && data.value[slot] !== '') deps[slot] = data.value[slot]

    updateFetchState(fieldKey, { loading: true, error: null })
    try {
        const result = await apiFetch<Record<string, unknown>[]>('/external/resolve', {
            method: 'POST',
            body: { component_key: props.componentKey, field: fieldKey, deps },
        })
        const items = Array.isArray(result) ? result : []
        updateFetchState(fieldKey, {
            loading: false,
            options: items.map(item => ({
                label: String(item[meta.label_key] ?? item[meta.value_key] ?? ''),
                value: String(item[meta.value_key] ?? ''),
            })),
        })
    }
    catch (e: any) {
        updateFetchState(fieldKey, {
            loading: false,
            error: e?.data?.detail ?? e?.message ?? 'Failed to fetch options',
            options: [],
        })
    }
}

/**
 * Check if the dependency (the provider's slot) for a fetch field is satisfied.
 */
function fetchDepsReady(meta: FetchMeta): boolean {
    const slot = fetchSlot(meta)
    if (mergedResourceContext.value[slot]) return true
    if (data.value[slot] !== undefined && data.value[slot] !== '') return true
    return false
}

/**
 * Build a fingerprint for a fetch field's dependency.
 * Used to detect when the slot's value actually changes and a re-fetch is needed.
 */
function depsFingerprint(meta: FetchMeta): string {
    const slot = fetchSlot(meta)
    const resourceData = mergedResourceContext.value[slot]
    if (resourceData) return `${slot}:resource`
    if (data.value[slot] !== undefined) return `${slot}:${data.value[slot]}`
    return ''
}

/** Track the last fingerprint per field to avoid redundant fetches. */
const lastFetchFingerprint = ref<Record<string, string>>({})

/**
 * Trigger fetch for all x-fetch fields whose dependencies are met.
 * Only re-fetches when the dependency fingerprint changes.
 */
watch(
    [mergedResourceContext, () => props.schema, data],
    () => {
        const properties = props.schema?.properties ?? {}
        for (const [key, prop] of Object.entries(properties)) {
            const meta = prop['x-fetch']
            if (!meta) continue
            if (!fetchDepsReady(meta)) continue
            const fp = depsFingerprint(meta)
            if (fp === lastFetchFingerprint.value[key]) continue
            lastFetchFingerprint.value[key] = fp
            fetchOptions(key, meta)
        }
    },
    { deep: true, immediate: true },
)

/** Resolve which widget to render for a given field. */
function resolveWidget(prop: JsonSchemaProperty): string {
    if (prop['x-widget']) return prop['x-widget']
    if (prop['x-options'] || prop['x-options-from'] || prop.enum) return 'select'
    switch (prop.type) {
        case 'boolean': return 'switch'
        case 'integer':
        case 'number': return 'number'
        default: return 'text'
    }
}

/** Resolve options for a field, including x-options-from context lookups. */
function resolveOptions(prop: JsonSchemaProperty): unknown[] | undefined {
    if (prop['x-options']) return prop['x-options']
    if (prop['x-options-from'] && props.optionsContext) {
        return props.optionsContext[prop['x-options-from']]
    }
    return prop.enum
}

/** Compute field descriptors from the schema. */
const fields = computed(() => {
    const properties = props.schema?.properties ?? {}
    const required = new Set(props.schema?.required ?? [])
    const excluded = new Set(props.exclude ?? ['id'])

    return Object.entries(properties)
        .filter(([key, prop]) => {
            if (excluded.has(key)) return false
            // Hide x-options-from fields when no options are available
            const entity = prop['x-options-from']
            if (entity) {
                const options = props.optionsContext?.[entity]
                if (!options?.length) return false
            }
            return true
        })
        .map(([key, prop]) => ({
            key,
            label: prop.title ?? key,
            description: prop.description,
            widget: resolveWidget(prop),
            required: required.has(key),
            default: prop.default,
            options: resolveOptions(prop),
            min: prop.minimum,
            max: prop.maximum,
            isOAuthField: oauthFieldKeys.value.has(key),
            fetchMeta: prop['x-fetch'] ?? null,
        }))
})

/** Fields visible in the current mode. */
const visibleFields = computed(() => {
    if (oauthAvailable.value && activeTab.value === 'oauth') {
        return fields.value.filter(f => !f.isOAuthField)
    }
    return fields.value
})

/** Handle OAuth sign-in success — map tokens to form fields. */
function handleOAuthSuccess(tokens: Record<string, unknown>) {
    if (!oauthMeta.value) return
    for (const [tokenKey, fieldKey] of Object.entries(oauthMeta.value.fields)) {
        if (tokens[tokenKey] !== undefined) {
            data.value[fieldKey] = tokens[tokenKey]
        }
    }
}

/** Initialise default values for fields that have them. */
function initDefaults() {
    for (const field of fields.value) {
        if (data.value[field.key] === undefined && field.default !== undefined) {
            data.value[field.key] = field.default
        }
    }
}

watch(
    () => props.schema,
    () => {
        initDefaults()
        // Reset fetch state when schema changes.
        fetchState.value = {}
        lastFetchFingerprint.value = {}
    },
    { immediate: true },
)

// Auto-select the first option for x-options-from fields when options become available.
watch(
    () => props.optionsContext,
    (ctx) => {
        if (!ctx) return
        const properties = props.schema?.properties ?? {}
        for (const [key, prop] of Object.entries(properties)) {
            const entity = prop['x-options-from']
            if (!entity) continue
            const options = ctx[entity]
            if (!options?.length) continue
            if (!data.value[key]) {
                data.value[key] = (options[0] as { value: string }).value
            }
        }
    },
    { immediate: true, deep: true },
)

/** Validate: all required fields must have a non-empty value. */
watch(
    data,
    (val) => {
        isValid.value = fields.value
            .filter(f => f.required)
            .every(f => val[f.key] !== undefined && val[f.key] !== '')
    },
    { deep: true, immediate: true },
)
</script>

<template>
    <div class="flex flex-col gap-4">
        <!-- OAuth tabs toggle -->
        <UTabs v-if="oauthAvailable"
               v-model="activeTab"
               :items="oauthTabs"
               :content="false"
               variant="pill"
               size="xs"
               class="flex" />

        <!-- Form fields -->
        <UFormField v-for="field in visibleFields"
                    :key="field.key"
                    :label="field.label"
                    :description="field.description"
                    :required="field.required">
            <!-- Password with visibility toggle -->
            <UInput v-if="field.widget === 'password'"
                    v-model="data[field.key]"
                    :type="revealedFields[field.key] ? 'text' : 'password'"
                    class="w-full">
                <template #trailing>
                    <UButton :icon="revealedFields[field.key] ? 'i-lucide-eye-off' : 'i-lucide-eye'"
                             color="neutral"
                             variant="link"
                             size="sm"
                             :padded="false"
                             @click="revealedFields[field.key] = !revealedFields[field.key]" />
                </template>
            </UInput>

            <!-- Textarea -->
            <UTextarea v-else-if="field.widget === 'textarea'"
                       v-model="data[field.key]"
                       class="w-full" />

            <!-- Number -->
            <UInput v-else-if="field.widget === 'number'"
                    v-model.number="data[field.key]"
                    type="number"
                    :min="field.min"
                    :max="field.max"
                    class="w-full" />

            <!-- Fetch select (x-fetch) — with deps available -->
            <div v-else-if="field.widget === 'fetch' && field.fetchMeta && fetchDepsReady(field.fetchMeta)"
                 class="flex flex-col gap-1.5 w-full">
                <div class="flex items-center gap-2">
                    <USelectMenu v-model="data[field.key]"
                                 :items="fetchState[field.key]?.options ?? []"
                                 value-key="value"
                                 :loading="fetchState[field.key]?.loading"
                                 :disabled="!fetchState[field.key]?.options?.length && !fetchState[field.key]?.loading"
                                 :placeholder="fetchState[field.key]?.loading ? 'Loading...' : 'Select...'"
                                 class="flex-1" />
                    <UButton icon="i-lucide-refresh-cw"
                             color="neutral"
                             variant="ghost"
                             size="xs"
                             :loading="fetchState[field.key]?.loading"
                             @click="fetchOptions(field.key, field.fetchMeta!)" />
                </div>
                <p v-if="fetchState[field.key]?.error"
                   class="text-xs text-error">
                    {{ fetchState[field.key]!.error }}
                </p>
            </div>

            <!-- Fetch field fallback — deps not available, degrade to text input -->
            <UInput v-else-if="field.widget === 'fetch'"
                    v-model="data[field.key]"
                    class="w-full" />

            <!-- Select (x-options or enum) -->
            <USelect v-else-if="field.widget === 'select'"
                     v-model="data[field.key]"
                     :items="(field.options ?? []).map((o: any) => typeof o === 'object' && o.label ? o : { label: String(o), value: o })"
                     class="w-full" />

            <!-- Switch (boolean) -->
            <USwitch v-else-if="field.widget === 'switch'"
                     v-model="data[field.key]" />

            <!-- JSON / code -->
            <UTextarea v-else-if="field.widget === 'json'"
                       :model-value="typeof data[field.key] === 'string' ? data[field.key] : JSON.stringify(data[field.key], null, 2)"
                       class="w-full font-mono"
                       :rows="6"
                       @update:model-value="(v: string) => { try { data[field.key] = JSON.parse(v) } catch { data[field.key] = v } }" />

            <!-- Default: text input -->
            <UInput v-else
                    v-model="data[field.key]"
                    class="w-full" />
        </UFormField>

        <!-- OAuth sign-in content -->
        <UFormField v-if="oauthAvailable && activeTab === 'oauth'"
                    label="OAuth"
                    description="Sign in with the OAuth provider to automatically fill in your credentials."
                    required>
            <OAuthSignIn :provider="oauthMeta!.provider"
                         :scope="oauthMeta!.scope"
                         :connected="oauthFilled"
                         @success="handleOAuthSuccess" />
        </UFormField>
    </div>
</template>
