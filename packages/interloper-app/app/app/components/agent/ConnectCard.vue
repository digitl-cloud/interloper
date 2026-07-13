<script setup lang="ts">
/**
 * Inline connection-setup card rendered in the agent chat.
 *
 * Triggered by the agent's `request_connection_setup` tool: reuses
 * SchemaForm (OAuth sign-in or manual credentials) so secrets go straight
 * from the form to the API, never through the conversation.
 *
 * Creation is gated on the candidate check (`POST /components/check`): the
 * create button runs the check first and only creates when it passes. A
 * failure is surfaced by the button itself — it turns into an error state
 * carrying the categorised message, and a click retries. Config failures
 * additionally land under the form fields via `SchemaForm.setErrors`; any
 * edit resets the button. On success the parent reports completion back
 * into the chat so the agent can continue.
 */
import type { FormError } from '@nuxt/ui'
import type { ConnectionSetupRequest } from '~/types/agent'

interface CheckResult {
    ok: boolean
    live: boolean
    message: string | null
    category: 'config' | 'auth' | 'network' | 'error' | null
    errors: { field: string, message: string }[]
}

const props = defineProps<{
    request: ConnectionSetupRequest
}>()

const emit = defineEmits<{
    created: [name: string, verified: boolean]
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const { apiFetch } = useApi()
const toast = useToast()

const defn = computed(() => catalogStore.catalog[props.request.connectionKey])
const schema = computed(() => (defn.value as any)?.config_schema ?? null)

const name = ref(props.request.name ?? '')
const formData = ref<Record<string, unknown>>({})
const formValid = ref(false)
const createdName = ref<string | null>(null)
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
                icon: 'i-lucide-circle-x',
            }
        default:
            return { label: 'Create connection', loading: false, color: 'primary' as const, icon: undefined }
    }
})

// A failure goes stale as soon as the user edits anything — back to idle.
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
        check = await apiFetch<CheckResult>('/components/check', {
            method: 'POST',
            body: { component_key: props.request.connectionKey, config: formData.value },
        })
    }
    catch (e: any) {
        check = {
            ok: false,
            live: false,
            category: 'error',
            message: e?.data?.detail ?? 'The connection check could not be run.',
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
        await componentsStore.create({
            kind: 'connection',
            key: props.request.connectionKey,
            name: name.value.trim(),
            config: formData.value,
        })
        createdName.value = name.value.trim()
        toast.add({ title: `Connection "${createdName.value}" created`, color: 'success' })
        emit('created', createdName.value, check.live)
    }
    catch {
        toast.add({ title: 'Failed to create connection', color: 'error' })
        state.value = 'idle'
    }
}
</script>

<template>
    <div class="border border-default rounded-[14px] p-5 my-2 w-full min-w-72 max-w-md">
        <!-- Unknown type: the catalog may not carry this key anymore -->
        <div v-if="!defn"
             class="flex items-center gap-2 text-sm text-muted">
            <UIcon name="i-lucide-circle-alert"
                   class="size-4 shrink-0" />
            Connection type "{{ request.connectionKey }}" is not in the catalog.
        </div>

        <!-- Created: locked summary -->
        <div v-else-if="createdName"
             class="flex items-center gap-3">
            <UIcon :name="componentIcon(request.connectionKey)"
                   class="size-6 shrink-0" />
            <div class="flex-1 text-sm">
                <span class="font-semibold">{{ createdName }}</span>
                <span class="text-muted"> — {{ defn.name }} connection created</span>
            </div>
            <UIcon name="i-lucide-check-circle-2"
                   class="size-5 text-success shrink-0" />
        </div>

        <!-- Setup form -->
        <div v-else
             class="flex flex-col gap-4">
            <div class="flex items-center gap-3">
                <UIcon :name="componentIcon(request.connectionKey)"
                       class="size-6 shrink-0" />
                <div class="text-sm font-semibold">
                    Set up {{ defn.name }} connection
                </div>
            </div>

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
    </div>
</template>
