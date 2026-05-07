<script setup lang="ts">
/**
 * Step for selecting or creating a resource instance.
 *
 * Lists existing instances matching the resource key. User picks one or
 * opens a nested drawer to create a new instance.
 *
 * Fetches its own matching instances on mount — no parent pre-fetch needed.
 */
import type { ComponentDefinition } from '~/types/catalog'
import type { Resource } from '~/types/resource'

const selectedId = defineModel<string>({ default: '' })

const props = withDefaults(defineProps<{
    /** The slot name (e.g. "connection"). */
    slotName: string
    /** The resource definition from the catalog. */
    definition: ComponentDefinition
    /**
     * Resource data from sibling steps, keyed by slot name.
     * Passed through to SchemaForm for x-fetch field resolution.
     */
    resourceContext?: Record<string, Record<string, unknown>> | undefined
    /** Suppress success toasts (e.g. when inside a collect-mode wizard). */
    silent?: boolean
}>(), {
    resourceContext: undefined,
    silent: false,
})

const emit = defineEmits<{
    created: [resource: Resource]
}>()

const resourcesStore = useResourcesStore()
const toast = useToast()

const drawerOpen = ref(false)
const newName = ref(props.definition.name ?? '')
const formData = ref<Record<string, any>>({})
const formValid = ref(false)
const creating = ref(false)
const loadingInstances = ref(false)

const schema = computed(() => (props.definition as any).config_schema ?? null)

const kindLabel = computed(() => {
    const s = props.slotName
    return s.charAt(0).toUpperCase() + s.slice(1)
})

// ── Instance loading ─────────────────────────────────────────────

/** Matching resource instances for this slot's key. */
const instances = computed(() =>
    resourcesStore.resources.filter(r => r.key === props.definition.key),
)

/** Fetch resources if the store is empty, then auto-select. */
onMounted(async () => {
    if (resourcesStore.resources.length === 0 && !resourcesStore.loading) {
        loadingInstances.value = true
        try {
            await resourcesStore.fetch()
        }
        finally {
            loadingInstances.value = false
        }
    }
    autoSelectSingle()
})

/** Auto-select when instances change (e.g. after fetch or after creating one). */
watch(instances, () => autoSelectSingle())

function autoSelectSingle() {
    if (selectedId.value) return
    if (instances.value.length === 1 && instances.value[0]) {
        selectedId.value = instances.value[0].id
    }
}

// ── Create ───────────────────────────────────────────────────────

const canCreate = computed(() =>
    !!newName.value.trim() && (formValid.value || !schema.value),
)

function openCreateDrawer() {
    newName.value = props.definition.name ?? ''
    formData.value = {}
    formValid.value = false
    drawerOpen.value = true
}

async function handleCreate() {
    if (!newName.value.trim()) return
    creating.value = true
    try {
        const resource = await resourcesStore.create({
            kind: props.definition.kind,
            key: props.definition.key,
            name: newName.value.trim(),
            data: formData.value,
            encrypted: false,
        })
        selectedId.value = resource.id
        drawerOpen.value = false
        emit('created', resource)
        if (!props.silent) toast.add({ title: `${props.definition.name} created`, color: 'success' })
    }
    catch {
        toast.add({ title: `Failed to create ${props.definition.name}`, color: 'error' })
    }
    finally {
        creating.value = false
    }
}

defineExpose({ formData })
</script>

<template>
    <!-- ── Loading ─────────────────────────────────────────────── -->
    <div v-if="loadingInstances"
         class="flex items-center justify-center py-8">
        <UIcon name="i-lucide-loader-circle"
               class="size-5 animate-spin text-muted" />
    </div>

    <!-- ── Select or create ─────────────────────────────────── -->
    <div v-else
         class="flex flex-col gap-4">
        <!-- Header -->
        <div class="flex items-center justify-between">
            <p class="text-sm text-muted">
                Select an existing <strong>{{ definition.name }}</strong> or create a new one.
            </p>
            <UButton size="xs"
                     variant="ghost"
                     icon="i-lucide-plus"
                     label="Create new"
                     @click="openCreateDrawer" />
        </div>

        <!-- Empty state -->
        <div v-if="instances.length === 0"
             class="flex flex-col items-center justify-center rounded-md p-6 gap-2">
            <span class="text-sm text-muted">No {{ definition.name }} {{ definition.kind.toLowerCase() }} found.</span>
            <UButton icon="i-lucide-plus"
                     label="Create one"
                     @click="openCreateDrawer" />
        </div>

        <!-- Instance list -->
        <div v-else
             class="flex flex-col gap-1">
            <div v-for="inst in instances"
                 :key="inst.id"
                 class="flex items-center gap-3 px-3 py-2.5 rounded-md cursor-pointer bg-elevated/50 hover:bg-elevated transition-colors"
                 :class="[
                     selectedId === inst.id ? 'ring-primary ring-2' : '',
                 ]"
                 @click="selectedId = inst.id">
                <UIcon :name="componentIcon(definition.key)"
                       class="size-5 shrink-0" />
                <div class="flex flex-col min-w-0">
                    <span class="text-sm font-medium">{{ inst.name }}</span>
                </div>
                <UIcon v-if="selectedId === inst.id"
                       name="i-lucide-check"
                       class="size-4 ml-auto text-primary shrink-0" />
            </div>
        </div>

        <!-- Create drawer (nested) -->
        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 nested
                 :handle="false"
                 :handle-only="true"
                 :title="`New ${kindLabel}`"
                 :ui="{ content: 'w-[36rem]', description: 'sr-only' }">
            <template #description>
                Create a new {{ definition.name }}
            </template>

            <template #body>
                <div class="flex flex-col gap-4">
                    <div class="flex items-center gap-2 text-sm text-muted">
                        <UIcon :name="componentIcon(definition.key)"
                               class="size-4" />
                        <span>{{ definition.name }}</span>
                    </div>

                    <UFormField label="Name"
                                required>
                        <UInput v-model="newName"
                                :placeholder=definition.name
                                class="w-full" />
                    </UFormField>

                    <USeparator v-if="schema" />

                    <SchemaForm v-if="schema"
                                v-model:data="formData"
                                v-model:is-valid="formValid"
                                :schema="schema"
                                :resource-context="resourceContext" />
                </div>
            </template>

            <template #footer>
                <div class="flex justify-end w-full">
                    <UButton :disabled="!canCreate || creating"
                             :loading="creating"
                             :label="`Create ${kindLabel}`"
                             @click="handleCreate" />
                </div>
            </template>
        </UDrawer>
    </div>
</template>
