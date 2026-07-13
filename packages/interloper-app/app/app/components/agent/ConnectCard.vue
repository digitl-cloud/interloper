<script setup lang="ts">
/**
 * Inline connection-setup card rendered in the agent chat.
 *
 * Triggered by the agent's `request_connection_setup` tool: reuses
 * SchemaForm (OAuth sign-in or manual credentials) so secrets go straight
 * from the form to the API, never through the conversation. On success the
 * parent reports completion back into the chat so the agent can continue.
 */
import type { ConnectionSetupRequest } from '~/types/agent'

const props = defineProps<{
    request: ConnectionSetupRequest
}>()

const emit = defineEmits<{
    created: [name: string]
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const toast = useToast()

const defn = computed(() => catalogStore.catalog[props.request.connectionKey])
const schema = computed(() => (defn.value as any)?.config_schema ?? null)

const name = ref(props.request.name ?? '')
const formData = ref<Record<string, unknown>>({})
const formValid = ref(false)
const submitting = ref(false)
const createdName = ref<string | null>(null)

watch(defn, (d) => {
    if (d && !name.value) name.value = `My ${d.name} Connection`
}, { immediate: true })

const canSubmit = computed(() => !!name.value.trim() && (formValid.value || !schema.value))

async function submit() {
    submitting.value = true
    try {
        await componentsStore.create({
            kind: 'connection',
            key: props.request.connectionKey,
            name: name.value.trim(),
            config: formData.value,
        })
        createdName.value = name.value.trim()
        toast.add({ title: `Connection "${createdName.value}" created`, color: 'success' })
        emit('created', createdName.value)
    }
    catch {
        toast.add({ title: 'Failed to create connection', color: 'error' })
    }
    submitting.value = false
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
                        v-model:data="formData"
                        v-model:is-valid="formValid"
                        :schema="schema" />

            <UButton :label="submitting ? 'Creating…' : 'Create connection'"
                     :loading="submitting"
                     :disabled="!canSubmit"
                     block
                     @click="submit" />
        </div>
    </div>
</template>
