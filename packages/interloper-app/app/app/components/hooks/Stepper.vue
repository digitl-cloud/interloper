<script setup lang="ts">
/**
 * Multi-step form for creating and editing hooks.
 *
 * Step 1: Type selection (create mode only)
 * Step 2: Watches (components the hook observes)
 * Step 3: Targets (components a trigger-style hook acts on)
 * Step 4: Details (name, events, per-type config, enabled)
 *
 * Container-agnostic: the parent wraps this in a UDrawer, modal, or
 * any other container. Navigation state is exposed via defineExpose.
 */
import type { StepperItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { hookEnabled, hookEvents, relationIds } from '~/types/component'

const props = defineProps<{
    /** Pass an existing hook to edit, or null to create. */
    hook: ComponentRecord | null
}>()

const emit = defineEmits<{
    created: []
    updated: []
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const definitions = computed(() => catalogStore.definitionsForKind('hook'))

// ── Form state ──────────────────────────────────────────────────
const selectedType = ref('')
const name = ref('')
const events = ref<string[]>(['run_failed'])
const enabled = ref(true)
const watchIds = ref<string[]>([])
const targetIds = ref<string[]>([])
const formData = ref<Record<string, any>>({})
const formValid = ref(false)
const submitting = ref(false)

const isEditing = computed(() => !!props.hook)

const EVENT_OPTIONS = [
    { label: 'Run completed', value: 'run_completed' },
    { label: 'Run failed', value: 'run_failed' },
]

/** Config fields owned by dedicated widgets, hidden from the schema form. */
const OWN_FIELDS = ['id', 'events', 'enabled']

// ── Data fetching ────────────────────────────────────────────────

onMounted(async () => {
    await componentsStore.fetchAll(['source', 'asset', 'job'])
    if (props.hook) {
        selectedType.value = props.hook.key
        name.value = props.hook.name ?? ''
        events.value = [...hookEvents(props.hook)]
        enabled.value = hookEnabled(props.hook)
        watchIds.value = relationIds(props.hook, 'watch')
        targetIds.value = relationIds(props.hook, 'target')
        formData.value = { ...props.hook.config }
    }
})

// ── Definition-derived pieces ────────────────────────────────────

const selectedDefinition = computed(() =>
    definitions.value.find(d => d.key === selectedType.value),
)

/** Candidates for a relation type, from the definition's allowed kinds. */
function relationCandidates(type: string): ComponentRecord[] {
    const kinds = selectedDefinition.value?.relations?.[type]?.kinds ?? []
    return kinds.flatMap(kind => componentsStore.byKind(kind))
}

const watchCandidates = computed(() => relationCandidates('watch'))
const targetCandidates = computed(() => relationCandidates('target'))

const selectedSchema = computed(() => (selectedDefinition.value as any)?.config_schema ?? null)

/** Whether the type has config fields beyond the shared events/enabled pair. */
const hasExtraFields = computed(() => {
    const properties = (selectedSchema.value?.properties ?? {}) as Record<string, unknown>
    return Object.keys(properties).some(key => !OWN_FIELDS.includes(key))
})

// When a type is selected (create mode only), reset form and advance
watch(selectedType, (newKey, oldKey) => {
    if (newKey && newKey !== oldKey && !isEditing.value) {
        formData.value = {}
        formValid.value = false
        name.value = `My ${selectedDefinition.value?.name ?? 'Hook'}`
        nextStep()
    }
})

// ── Stepper ─────────────────────────────────────────────────────
// Only hook classes whose definition declares the `target` relation
// (trigger-style hooks) act on other components — the step follows.
const hasTargets = computed(() => !!selectedDefinition.value?.relations?.target)

const steps = computed<StepperItem[]>(() => {
    const items: StepperItem[] = []
    if (!isEditing.value) {
        items.push({ title: 'Type', icon: 'i-lucide-shapes', slot: 'type' as const })
    }
    items.push({ title: 'Watches', icon: 'i-lucide-eye', slot: 'watches' as const })
    if (hasTargets.value) {
        items.push({ title: 'Targets', icon: 'i-lucide-crosshair', slot: 'targets' as const })
    }
    items.push({ title: 'Details', icon: 'i-lucide-settings-2', slot: 'details' as const })
    return items
})

const { activeStep, hasPrev, isLastStep, next: nextStep, prev: prevStep } = useStepperFlow(computed(() => steps.value.length))

const displaySteps = useCheckedSteps(steps, activeStep)

/** Recap of the components picked on earlier steps. */
const recapRows = computed(() => {
    const names = (ids: string[]) => ids
        .map(id => componentsStore.byId(id)?.name)
        .filter(Boolean)
        .join(', ')
    const rows = [{ icon: 'i-lucide-eye', label: 'Watches', value: names(watchIds.value) || 'None' }]
    if (hasTargets.value) {
        rows.push({ icon: 'i-lucide-crosshair', label: 'Targets', value: names(targetIds.value) || 'None' })
    }
    return rows
})

// ── Validation ──────────────────────────────────────────────────
const detailsValid = computed(() =>
    !!name.value.trim()
    && events.value.length > 0
    && (!hasExtraFields.value || formValid.value),
)

const canProceed = computed(() => {
    const slot = steps.value[activeStep.value]?.slot
    if (slot === 'type') return !!selectedType.value
    if (slot === 'watches') return watchIds.value.length > 0
    if (slot === 'targets') return true
    if (slot === 'details') return detailsValid.value
    return false
})

// ── Submit ──────────────────────────────────────────────────────
async function submit() {
    submitting.value = true
    try {
        const input = {
            name: name.value.trim(),
            config: {
                ...formData.value,
                events: [...events.value],
                enabled: enabled.value,
            },
            relations: {
                watch: watchIds.value.map(id => ({ dst_id: id })),
                ...(hasTargets.value ? { target: targetIds.value.map(id => ({ dst_id: id })) } : {}),
            },
        }

        if (props.hook) {
            await componentsStore.update(props.hook.id, input)
            toast.add({ title: 'Hook updated', color: 'success' })
            emit('updated')
        }
        else {
            await componentsStore.create({ ...input, kind: 'hook', key: selectedType.value })
            toast.add({ title: 'Hook created', color: 'success' })
            emit('created')
        }
    }
    catch {
        toast.add({ title: `Failed to ${isEditing.value ? 'update' : 'create'} hook`, color: 'error' })
    }
    finally {
        submitting.value = false
    }
}

function handleNext() {
    if (isLastStep.value) submit()
    else nextStep()
}

// ── Expose navigation state ──────────────────────────────────────

const title = computed(() => isEditing.value ? 'Edit Hook' : 'New Hook')
const submitLabel = computed(() => {
    if (!isLastStep.value) return 'Next'
    return isEditing.value ? 'Save Hook' : 'Create Hook'
})

defineExpose({ canProceed, hasPrev, isLastStep, submitting, submitLabel, title, next: handleNext, prev: prevStep })
</script>

<template>
    <UStepper v-model="activeStep"
              :items="displaySteps"
              linear
              disabled
              class="w-full">
        <!-- Type (create mode only) -->
        <template #type>
            <TypeSelect v-model="selectedType"
                        :definitions="definitions" />
        </template>

        <!-- Watches -->
        <template #watches>
            <div class="flex flex-col gap-4">
                <p class="text-sm text-muted">
                    Components this hook observes — it fires when one of their runs matches the selected events.
                </p>
                <HooksComponentSelect v-model="watchIds"
                                      :components="watchCandidates"
                                      noun="watched" />
            </div>
        </template>

        <!-- Targets -->
        <template #targets>
            <div class="flex flex-col gap-4">
                <p class="text-sm text-muted">
                    Components a trigger-style hook runs when it fires. Optional — leave empty for hooks that only
                    notify.
                </p>
                <HooksComponentSelect v-model="targetIds"
                                      :components="targetCandidates"
                                      noun="targeted" />
            </div>
        </template>

        <!-- Details -->
        <template #details>
            <div class="flex flex-col gap-6">
                <WizardRecap :rows="recapRows" />

                <UFormField label="Hook name"
                            required>
                    <UInput v-model="name"
                            placeholder="My hook"
                            class="w-full" />
                </UFormField>

                <UFormField label="Events"
                            description="Run outcomes this hook reacts to."
                            required>
                    <USelectMenu v-model="events"
                                 :items="EVENT_OPTIONS"
                                 value-key="value"
                                 multiple
                                 placeholder="Select events..."
                                 class="w-full" />
                </UFormField>

                <template v-if="hasExtraFields">
                    <USeparator label="Configuration" />

                    <SchemaForm v-model:data="formData"
                                v-model:is-valid="formValid"
                                :schema="selectedSchema!"
                                :exclude="OWN_FIELDS" />
                </template>

                <USeparator label="Options" />

                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium">Enabled</p>
                        <p class="text-xs text-muted">Hook will fire on matching events.</p>
                    </div>
                    <USwitch v-model="enabled" />
                </div>
            </div>
        </template>
    </UStepper>
</template>
