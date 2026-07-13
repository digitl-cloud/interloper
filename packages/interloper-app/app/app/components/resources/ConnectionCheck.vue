<script setup lang="ts">
/**
 * Runs `POST /components/check` for a connection's candidate config and shows
 * the outcome. Auto-runs on mount; with `manual`, renders a "Test connection"
 * button instead. A failed check is informative, never blocking — the parent
 * decides what to do with the emitted result.
 *
 * Static (per-field) validation errors are not rendered here: they are
 * emitted as `field-errors` for the parent to surface under the form's
 * fields (via `SchemaForm.setErrors`), the Nuxt UI form-validation way.
 */
import type { FormError } from '@nuxt/ui'

interface CheckFieldError {
    field: string
    message: string
}

interface CheckResult {
    ok: boolean
    live: boolean
    message: string | null
    category: 'config' | 'auth' | 'network' | 'error' | null
    errors: CheckFieldError[]
}

const props = withDefaults(defineProps<{
    /** The connection type's catalog key. */
    componentKey: string
    /** Candidate config, as assembled by the form. */
    config: Record<string, unknown>
    /** Render a "Test connection" button instead of auto-running on mount. */
    manual?: boolean
}>(), {
    manual: false,
})

const emit = defineEmits<{
    result: [ok: boolean]
    fieldErrors: [errors: FormError[]]
}>()

const { apiFetch } = useApi()

const checking = ref(false)
const result = ref<CheckResult | null>(null)

async function run() {
    checking.value = true
    result.value = null
    emit('fieldErrors', [])
    try {
        result.value = await apiFetch<CheckResult>('/components/check', {
            method: 'POST',
            body: { component_key: props.componentKey, config: props.config },
        })
    }
    catch (e: any) {
        result.value = {
            ok: false,
            live: false,
            category: 'error',
            message: e?.data?.detail ?? 'The connection check could not be run.',
            errors: [],
        }
    }
    finally {
        checking.value = false
        if (result.value) {
            emit('result', result.value.ok)
            emit('fieldErrors', result.value.errors.map(e => ({ name: e.field, message: e.message })))
        }
    }
}

onMounted(() => {
    if (!props.manual) run()
})

// A manual result goes stale as soon as the user edits the config — clear
// both the outcome and any field errors it placed on the form.
watch(() => props.config, () => {
    if (props.manual && !checking.value && result.value) {
        result.value = null
        emit('fieldErrors', [])
    }
}, { deep: true })

defineExpose({ run, checking, result })
</script>

<template>
    <div class="flex flex-col gap-3">
        <!-- Manual mode: a persistent trigger — re-running replaces Retry. -->
        <UButton v-if="manual"
                 block
                 variant="outline"
                 icon="i-lucide-radio-tower"
                 label="Test connection"
                 :loading="checking"
                 @click="run" />
        <UAlert v-else-if="checking"
                color="info"
                variant="subtle"
                icon="i-lucide-loader-circle"
                title="Testing connection"
                description="Making a lightweight call to the provider..." />

        <template v-if="!checking">
            <UAlert v-if="result?.ok"
                    color="success"
                    variant="subtle"
                    icon="i-lucide-circle-check"
                    title="Connection verified"
                    :description="result.live ? 'The provider accepted the credentials.' : 'The configuration is valid.'" />

            <!-- Config failures surface under the form fields, not as a card. -->
            <UAlert v-else-if="result && result.category !== 'config'"
                    color="error"
                    variant="subtle"
                    icon="i-lucide-circle-x"
                    title="Connection check failed"
                    :description="result.message ?? 'The connection check failed.'" />
        </template>
    </div>
</template>
