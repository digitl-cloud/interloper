<script setup lang="ts">
/**
 * Runs `POST /external/check` for a connection's candidate config and shows
 * the outcome. Auto-runs on mount; with `manual`, renders a "Test connection"
 * button instead. A failed check is informative, never blocking — the parent
 * decides what to do with the emitted result.
 */
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
}>()

const { apiFetch } = useApi()

const checking = ref(false)
const result = ref<CheckResult | null>(null)

const failureHint: Record<string, string> = {
    config: 'Fix the highlighted fields and try again.',
    auth: 'Check the credentials, or run the sign-in again.',
    network: 'The provider may be down — try again in a moment.',
    error: 'Retry, or create the connection anyway and investigate later.',
}

async function run() {
    checking.value = true
    result.value = null
    try {
        result.value = await apiFetch<CheckResult>('/external/check', {
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
        if (result.value) emit('result', result.value.ok)
    }
}

onMounted(() => {
    if (!props.manual) run()
})

// A manual result goes stale as soon as the user edits the config.
watch(() => props.config, () => {
    if (props.manual && !checking.value) result.value = null
}, { deep: true })

defineExpose({ run, checking, result })
</script>

<template>
    <div class="flex flex-col gap-3">
        <UAlert v-if="checking"
                color="info"
                variant="subtle"
                icon="i-lucide-loader-circle"
                title="Testing connection"
                description="Making a lightweight call to the provider..." />

        <UAlert v-else-if="result?.ok"
                color="success"
                variant="subtle"
                icon="i-lucide-circle-check"
                title="Connection verified"
                :description="result.live ? 'The provider accepted the credentials.' : 'The configuration is valid.'" />

        <template v-else-if="result">
            <UAlert color="error"
                    variant="subtle"
                    icon="i-lucide-circle-x"
                    title="Connection check failed"
                    :description="result.message ?? 'The connection check failed.'">
                <template v-if="result.errors.length"
                          #description>
                    <div class="flex flex-col gap-1">
                        <span>{{ result.message ?? 'The configuration is invalid.' }}</span>
                        <ul class="list-disc list-inside">
                            <li v-for="err in result.errors"
                                :key="err.field">
                                <span class="font-medium">{{ err.field }}</span>: {{ err.message }}
                            </li>
                        </ul>
                    </div>
                </template>
            </UAlert>
            <div class="flex items-center justify-between">
                <span class="text-sm text-muted">{{ failureHint[result.category ?? 'error'] }}</span>
                <UButton size="xs"
                         variant="ghost"
                         icon="i-lucide-refresh-cw"
                         label="Retry"
                         @click="run" />
            </div>
        </template>

        <div v-else-if="manual"
             class="flex justify-start">
            <UButton size="xs"
                     variant="outline"
                     icon="i-lucide-radio-tower"
                     label="Test connection"
                     @click="run" />
        </div>
    </div>
</template>
