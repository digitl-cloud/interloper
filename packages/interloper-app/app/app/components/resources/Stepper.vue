<script setup lang="ts">
/**
 * Two-step form for creating and editing resources (connections, configs).
 *
 * Step 1: Type selection (create mode only)
 * Step 2: Details (name + schema form; checkable connections get a
 *         "Test connection" button — failures warn but never block saving)
 *
 * Container-agnostic: the parent wraps this in a UDrawer, modal, or
 * any other container. Navigation state is exposed via defineExpose.
 */
import type { StepperItem } from '@nuxt/ui'
import type { ComponentDefinition } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

const props = defineProps<{
    /** The resource kind (e.g. "connection", "config"). */
    kind: string
    /** Available type definitions for this kind. */
    definitions: ComponentDefinition[]
    /** When set, opens in edit mode for this resource. */
    resource?: ComponentRecord | null
    /** Preselect this type and open directly on the details step (create mode). */
    initialTypeKey?: string
}>()

const emit = defineEmits<{
    created: []
    updated: []
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const toast = useToast()

const selectedType = ref('')
const resourceName = ref('')
const formData = ref<Record<string, unknown>>({})
const formValid = ref(false)
const submitting = ref(false)
const loadingEdit = ref(false)

/** Whether we're in edit mode. */
const isEditing = computed(() => !!props.resource)

/** The selected type's catalog definition. */
const selectedDefinition = computed(() => props.definitions.find(d => d.key === selectedType.value))

/** Whether the selected type supports a live connection check. */
const checkable = computed(() => !!selectedDefinition.value?.checkable)

const steps: StepperItem[] = [
    { slot: 'type' as const, title: 'Type', icon: 'i-lucide-plug' },
    { slot: 'details' as const, title: 'Details', icon: 'i-lucide-settings-2' },
]
const { activeStep, hasPrev, hasNext, reset: resetStepper, next: nextStep, prev: prevStep } = useStepperFlow(2)
const displaySteps = useCheckedSteps(steps, activeStep)

/** Label for the schema section separator. */
const schemaSectionLabel = computed(() => props.kind === 'connection' ? 'Credentials' : 'Configuration')

/** The config_schema for the currently selected type. */
const selectedSchema = computed(() => {
    if (!selectedType.value) return null
    const defn = catalogStore.catalog[selectedType.value]
    return (defn as any)?.config_schema ?? null
})

/** Kind label (e.g. "Connection"). */
const kindLabel = computed(() => {
    const k = props.kind
    return k.charAt(0).toUpperCase() + k.slice(1)
})

/** Load the full resource data for editing (decoded config comes from the detail fetch). */
async function loadResourceData(resource: ComponentRecord) {
    loadingEdit.value = true
    try {
        const detail = await componentsStore.fetchOne(resource.id)
        selectedType.value = resource.key
        resourceName.value = resource.name ?? ''
        formData.value = detail.config ?? {}
        activeStep.value = 1
    }
    finally {
        loadingEdit.value = false
    }
}

// ── Data fetching ────────────────────────────────────────────────

onMounted(() => {
    if (props.resource) {
        loadResourceData(props.resource)
    }
    else if (props.initialTypeKey) {
        // Triggers the selection watcher below, which advances to details.
        selectedType.value = props.initialTypeKey
    }
})

// When a type is selected (create mode only), reset form and advance
watch(selectedType, (newKey, oldKey) => {
    if (newKey && newKey !== oldKey && !isEditing.value) {
        formData.value = {}
        formValid.value = false
        const defn = props.definitions.find(d => d.key === newKey)
        resourceName.value = `My ${defn?.name ?? ''} ${kindLabel.value}`
        nextStep()
    }
})

/** Resolve display name from selected type key. */
const selectedTypeName = computed(() => {
    if (!selectedType.value) return ''
    return selectedDefinition.value?.name ?? selectedType.value
})

/** First catalog tag of the selected type (summary-card caption). */
const selectedTypeTag = computed(() => selectedDefinition.value?.tags?.[0] ?? kindLabel.value)

/** Can proceed to next step? */
const canNext = computed(() => !!selectedType.value)

/** Can submit? */
const canSubmit = computed(() => !!resourceName.value.trim() && (formValid.value || !selectedSchema.value))

// ── Validation ───────────────────────────────────────────────────

const canProceed = computed(() => {
    if (isEditing.value) return canSubmit.value
    if (hasNext.value) return canNext.value
    return canSubmit.value
})

// ── Submit ───────────────────────────────────────────────────────

async function submit() {
    submitting.value = true
    try {
        if (isEditing.value && props.resource) {
            await componentsStore.update(props.resource.id, {
                name: resourceName.value.trim(),
                config: formData.value,
            })
            toast.add({ title: `${kindLabel.value} "${resourceName.value}" updated`, color: 'success' })
            emit('updated')
        }
        else {
            await componentsStore.create({
                kind: props.kind,
                key: selectedType.value,
                name: resourceName.value.trim(),
                config: formData.value,
            })
            toast.add({ title: `${kindLabel.value} "${resourceName.value}" created`, color: 'success' })
            emit('created')
        }
    }
    catch {
        toast.add({ title: `Failed to ${isEditing.value ? 'update' : 'create'} ${props.kind}`, color: 'error' })
    }
    submitting.value = false
}

function handleNext() {
    if (isEditing.value || !hasNext.value) submit()
    else nextStep()
}

// ── Expose navigation state ──────────────────────────────────────

const title = computed(() => isEditing.value ? `Edit ${kindLabel.value}` : `New ${kindLabel.value}`)
const submitLabel = computed(() => {
    if (isEditing.value) return `Save ${kindLabel.value}`
    if (hasNext.value) return 'Next'
    return `Create ${kindLabel.value}`
})

defineExpose({ canProceed, hasPrev: computed(() => !isEditing.value && hasPrev.value), isLastStep: computed(() => isEditing.value || !hasNext.value), submitting, submitLabel, title, next: handleNext, prev: prevStep })
</script>

<template>
    <!-- Edit mode: loading -->
    <div v-if="loadingEdit"
         class="flex items-center justify-center py-12">
        <UIcon name="i-lucide-loader-2"
               class="size-6 animate-spin text-muted" />
    </div>

    <!-- Create mode: stepper -->
    <UStepper v-else-if="!isEditing"
              v-model="activeStep"
              :items="displaySteps"
              linear
              disabled
              class="w-full">
        <template #type>
            <TypeSelect v-model="selectedType"
                        :definitions="definitions" />
        </template>

        <template #details>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard :icon="componentIcon(selectedType)"
                                 :title="selectedTypeName"
                                 :caption="selectedTypeTag"
                                 changeable
                                 @change="prevStep" />

                <UFormField :label="`${kindLabel} name`"
                            required>
                    <UInput v-model="resourceName"
                            placeholder="Enter a name for this resource"
                            class="w-full" />
                </UFormField>

                <USeparator :label="schemaSectionLabel" />

                <div v-if="selectedSchema"
                     class="space-y-1">
                    <SchemaForm v-model:data="formData"
                                v-model:is-valid="formValid"
                                :schema="selectedSchema" />
                </div>
                <div v-else
                     class="text-sm text-muted italic">
                    No configuration required for this type.
                </div>

                <ResourcesConnectionCheck v-if="checkable"
                                          :component-key="selectedType"
                                          :config="formData"
                                          manual />
            </div>
        </template>
    </UStepper>

    <!-- Edit mode: details form only -->
    <div v-else
         class="flex flex-col gap-6">
        <TypeSummaryCard :icon="componentIcon(selectedType)"
                         :title="selectedTypeName"
                         :caption="selectedTypeTag" />

        <UFormField :label="`${kindLabel} name`"
                    required>
            <UInput v-model="resourceName"
                    placeholder="Enter a name for this resource"
                    class="w-full" />
        </UFormField>

        <USeparator :label="schemaSectionLabel" />

        <div v-if="selectedSchema"
             class="space-y-1">
            <SchemaForm v-model:data="formData"
                        v-model:is-valid="formValid"
                        :schema="selectedSchema" />
        </div>
        <div v-else
             class="text-sm text-muted italic">
            No configuration required for this type.
        </div>

        <ResourcesConnectionCheck v-if="checkable"
                                  :component-key="selectedType"
                                  :config="formData"
                                  manual />
    </div>
</template>
