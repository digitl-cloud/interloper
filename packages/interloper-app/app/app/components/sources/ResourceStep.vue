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
import type { ComponentRecord } from '~/types/component'

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
    created: [resource: ComponentRecord]
}>()

const componentsStore = useComponentsStore()
const toast = useToast()

const drawerOpen = ref(false)
const newName = ref(props.definition.name ?? '')
const formData = ref<Record<string, any>>({})
const formValid = ref(false)
/** Whether the schema form is in manual credential entry (vs OAuth sign-in). */
const manualCreds = ref(true)
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
    componentsStore.byKind(props.definition.kind).filter(r => r.key === props.definition.key),
)

/** Fetch resources if the store has none of this kind yet, then auto-select. */
onMounted(async () => {
    if (instances.value.length === 0 && !componentsStore.loading) {
        loadingInstances.value = true
        try {
            await componentsStore.fetchAll([props.definition.kind])
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
        const resource = await componentsStore.create({
            kind: props.definition.kind,
            key: props.definition.key,
            name: newName.value.trim(),
            config: formData.value,
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
        <InlineEmptyState v-if="instances.length === 0"
                          :icon="componentIcon(definition.key)"
                          :message="`No ${definition.name} ${definition.kind.toLowerCase()} yet.`"
                          :action-label="`Create ${definition.kind.toLowerCase()}`"
                          @action="openCreateDrawer" />

        <!-- Instance list -->
        <div v-else
             class="flex flex-col gap-2.5">
            <SelectionCard v-for="inst in instances"
                           :key="inst.id"
                           :selected="selectedId === inst.id"
                           class="flex items-center gap-3 px-4 py-3"
                           @select="selectedId = inst.id">
                <div class="size-10 shrink-0 rounded-[11px] border border-default bg-default flex items-center justify-center">
                    <UIcon :name="componentIcon(definition.key)"
                           class="size-6" />
                </div>
                <span class="text-[14.5px] font-semibold text-highlighted truncate">{{ inst.name }}</span>
                <UIcon v-if="selectedId === inst.id"
                       name="i-lucide-check"
                       class="size-4 ml-auto text-primary shrink-0" />
            </SelectionCard>
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
                                :placeholder="definition.name"
                                class="w-full" />
                    </UFormField>

                    <USeparator v-if="schema" />

                    <SchemaForm v-if="schema"
                                v-model:data="formData"
                                v-model:is-valid="formValid"
                                v-model:manual-mode="manualCreds"
                                :schema="schema"
                                :resource-context="resourceContext" />

                    <ResourcesConnectionCheck v-if="definition.checkable && manualCreds"
                                              :component-key="definition.key"
                                              :config="formData"
                                              manual />
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
