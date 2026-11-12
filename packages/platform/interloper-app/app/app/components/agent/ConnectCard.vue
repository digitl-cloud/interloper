<script setup lang="ts">
/**
 * Inline connection-setup card rendered in the agent chat.
 *
 * Triggered by the agent's `request_connection_setup` tool call: reuses
 * SchemaForm (OAuth sign-in or manual credentials) so secrets go straight
 * from the form to the API, never through the conversation.
 *
 * Creation is gated on the candidate check (`POST /catalog/{key}/check`): the
 * create button runs the check first and only creates when it passes. A
 * failure is surfaced by the button itself: it turns into an error state
 * carrying the categorised message, and a click retries. Config failures
 * additionally land under the form fields via `SchemaForm.setErrors`; any
 * edit resets the button. The created connection becomes the tool call's
 * output, and the agent continues with it; a call already answered
 * (`answer`) renders locked.
 */
import type { FormError } from '@nuxt/ui'
import type { ConnectionSetupRequest, ConnectionSetupResult } from '~/types/agent'

interface CheckResult {
    ok: boolean
    live: boolean
    message: string | null
    category: 'config' | 'auth' | 'network' | 'error' | null
    errors: { field: string, message: string }[]
}

const props = defineProps<{
    request: ConnectionSetupRequest
    answer?: ConnectionSetupResult
}>()

const emit = defineEmits<{
    created: [result: ConnectionSetupResult]
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { apiFetch } = useOrgApi()
const toast = useToast()
const appConfig = useAppConfig()

const defn = computed(() => catalogStore.catalog[props.request.connection_key])
const icon = computed(() => componentIcon(props.request.connection_key, appConfig.icons.connection))
const schema = computed(() => (defn.value as any)?.config_schema ?? null)

const name = ref(props.request.name ?? '')
const formData = ref<Record<string, unknown>>({})
const formValid = ref(false)
const createdName = ref<string | null>(props.answer?.name ?? null)
const schemaForm = ref<{ setErrors: (errors: FormError[]) => void } | null>(null)

/** The submit button drives the whole flow, including failure display. */
const state = ref<'idle' | 'checking' | 'creating' | 'failed'>('idle')
const failure = ref<CheckResult | null>(null)

watch(defn, (d) => {
    if (d && !name.value) name.value = `My ${d.name} Connection`
}, { immediate: true })

const canSubmit = computed(() => !!name.value.trim() && (formValid.value || !schema.value))

const button = computed(() => {
    switch (state.value) {
        case 'checking':
            return { label: 'Testing connection…', loading: true, color: 'primary' as const, icon: undefined }
        case 'creating':
            return { label: 'Creating…', loading: true, color: 'primary' as const, icon: undefined }
        case 'failed':
            return {
                label: failure.value?.category === 'config'
                    ? 'Fix the highlighted fields'
                    : `${failure.value?.message ?? 'The connection check failed.'} Try again`,
                loading: false,
                color: 'error' as const,
                icon: appConfig.ui.icons.error,
            }
        default:
            return { label: 'Create connection', loading: false, color: 'primary' as const, icon: undefined }
    }
})

// A failure goes stale as soon as the user edits anything: back to idle.
watch([formData, name], () => {
    if (state.value === 'failed') {
        state.value = 'idle'
        failure.value = null
        schemaForm.value?.setErrors([])
    }
}, { deep: true })

async function submit() {
    state.value = 'checking'
    failure.value = null
    schemaForm.value?.setErrors([])

    let check: CheckResult
    try {
        check = await apiFetch<CheckResult>(`/catalog/${props.request.connection_key}/check`, {
            method: 'POST',
            body: { config: formData.value },
        })
    }
    catch (e) {
        check = {
            ok: false,
            live: false,
            category: 'error',
            message: errorDetail(e) ?? 'The connection check could not be run.',
            errors: [],
        }
    }

    if (!check.ok) {
        failure.value = check
        state.value = 'failed'
        schemaForm.value?.setErrors(check.errors.map(e => ({ name: e.field, message: e.message })))
        return
    }

    state.value = 'creating'
    try {
        const created = await componentsStore.create({
            kind: 'connection',
            key: props.request.connection_key,
            name: name.value.trim(),
            config: formData.value,
        })
        createdName.value = name.value.trim()
        toast.add({ title: 'Connection created', description: createdName.value, color: 'success' })
        emit('created', { connection_id: created.id, name: createdName.value, verified: check.live })
    }
    catch (e) {
        toast.add(errorToast(e, 'Couldn\'t create connection'))
        state.value = 'idle'
    }
}
</script>

<template>
    <!-- Unknown type: the catalog may not carry this key anymore -->
    <AgentCard v-if="!defn">
        <p class="flex items-center gap-2 text-sm text-muted">
            <UIcon :name="appConfig.ui.icons.caution"
                   class="size-4 shrink-0" />
            Connection type "{{ request.connection_key }}" is not in the catalog.
        </p>
    </AgentCard>

    <!-- Created: locked summary -->
    <AgentCard v-else-if="createdName"
               :icon="icon"
               :title="createdName"
               :description="`${defn.name} connection created`">
        <template #actions>
            <UIcon :name="appConfig.ui.icons.success"
                   class="size-5 shrink-0 text-success" />
        </template>
    </AgentCard>

    <!-- Setup form -->
    <AgentCard v-else
               :icon="icon"
               :title="defn.name"
               description="New connection">
        <div class="flex flex-col gap-3">
            <UFormField label="Connection name"
                        required>
                <UInput v-model="name"
                        placeholder="Enter a name for this connection"
                        class="w-full" />
            </UFormField>

            <SchemaForm v-if="schema"
                        ref="schemaForm"
                        v-model:data="formData"
                        v-model:is-valid="formValid"
                        :schema="schema" />

            <UButton :label="button.label"
                     :loading="button.loading"
                     :color="button.color"
                     :icon="button.icon"
                     :disabled="!canSubmit"
                     block
                     @click="submit" />
        </div>
    </AgentCard>
</template>
