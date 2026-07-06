<script setup lang="ts">
/**
 * Multi-step form for creating or editing a destination.
 *
 * Steps are dynamic based on the selected destination definition:
 *   1. Destination type (always, skip in edit mode)
 *   2..N. One step per resource slot (e.g. connection)
 *   N+1. Config (if destination has config fields)
 *
 * Container-agnostic: the parent wraps this in a UDrawer or any container.
 * Navigation state is exposed via defineExpose.
 */
import type { StepperItem } from '@nuxt/ui'
import type { ComponentRecord, ComponentInput } from '~/types/component'
import { resourceMap } from '~/types/component'
import type { DestinationDefinition } from '~/types/catalog'
import { resourceSlots as definitionResourceSlots } from '~/types/catalog'

const props = withDefaults(defineProps<{
    /** 'standalone' saves to API, 'collect' emits config without saving. */
    mode?: 'standalone' | 'collect'
    /** Compatible destination keys. Empty = all types available. */
    compatibleKeys?: string[]
    /** When set, stepper opens in edit mode with values pre-filled. */
    destination?: ComponentRecord | null
    /** Preselect this type and open directly on the next step (create mode). */
    initialTypeKey?: string
}>(), {
    mode: 'standalone',
    compatibleKeys: () => [],
    destination: null,
    initialTypeKey: undefined,
})

const emit = defineEmits<{
    created: []
    updated: []
    collected: [config: { key: string; name: string; config: Record<string, any>; resources: Record<string, string> }]
}>()

const isEditMode = computed(() => !!props.destination)

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const toast = useToast()

// ── State ────────────────────────────────────────────────────────
const selectedDestKey = ref('')
const destName = ref('')
const resourceSelections = ref<Record<string, string>>({})
const configData = ref<Record<string, any>>({})
const configValid = ref(true)
const submitting = ref(false)

/** Template refs for ResourceStep components, keyed by slot name. */
const resourceStepRefs = ref<Record<string, any>>({})

// ── Edit mode pre-fill ──────────────────────────────────────────

if (props.destination) {
    selectedDestKey.value = props.destination.key
    destName.value = props.destination.name ?? ''
    configData.value = { ...(props.destination.config || {}) }
    resourceSelections.value = resourceMap(props.destination)
}

// ── Derived ──────────────────────────────────────────────────────

/** Available destination types, filtered by compatible keys if provided. */
const availableDefinitions = computed(() => {
    const all = catalogStore.destinationDefinitions
    if (!props.compatibleKeys.length) return all
    return all.filter(d => props.compatibleKeys.includes(d.key))
})

/** The selected destination definition. */
const destDefn = computed<DestinationDefinition | undefined>(() =>
    selectedDestKey.value ? catalogStore.getDestinationDefinition(selectedDestKey.value) : undefined,
)

/** Resource slots: entries from the destination's `resource` relation slots. */
const resourceSlots = computed(() => {
    if (!destDefn.value) return []
    return Object.entries(definitionResourceSlots(destDefn.value)).map(([slotName, resourceKey]) => ({
        slotName,
        resourceKey,
        definition: catalogStore.catalog[resourceKey],
    }))
})

/** Whether the destination has user-facing config fields. */
const hasConfig = computed(() => {
    const schema = destDefn.value?.config_schema
    return schema && Object.keys((schema as any).properties || {}).length > 0
})

/** Dynamic stepper items. */
const steps = computed<StepperItem[]>(() => {
    const items: StepperItem[] = []

    // Destination type (skip in edit mode)
    if (!isEditMode.value) {
        items.push({ title: 'Destination', icon: 'i-lucide-hard-drive', slot: 'destination' as const })
    }

    // Resource steps
    for (const rs of resourceSlots.value) {
        if (!rs.definition) continue
        const kind = rs.slotName.charAt(0).toUpperCase() + rs.slotName.slice(1)
        items.push({
            title: kind,
            icon: resourceSlotIcon(rs.slotName),
            slot: `resource-${rs.slotName}` as any,
        })
    }

    // Config step
    if (hasConfig.value) {
        items.push({ title: 'Config', icon: 'i-lucide-settings-2', slot: 'config' as const })
    }

    return items
})

const totalSteps = computed(() => steps.value.length)
const { activeStep, hasPrev, isLastStep, next: nextStep, prev: prevStep } = useStepperFlow(totalSteps)

const displaySteps = useCheckedSteps(steps, activeStep)

/** Selected-type summary card shown on every post-type step. */
const summaryCard = computed(() => destDefn.value && {
    icon: componentIcon(destDefn.value.key),
    title: destDefn.value.name,
    caption: destDefn.value.tags?.[0] ?? 'Destination',
    changeable: !isEditMode.value,
})

/** Recap rows for the final step — the resources chosen along the way. */
const recapRows = computed(() => resourceSlots.value.map((rs) => {
    const id = resourceSelections.value[rs.slotName]
    return {
        icon: resourceSlotIcon(rs.slotName),
        label: rs.slotName.charAt(0).toUpperCase() + rs.slotName.slice(1),
        value: id ? (componentsStore.byId(id)?.name ?? '—') : 'None',
    }
}))

// ── Auto-advance on type selection ──────────────────────────────

watch(selectedDestKey, (key) => {
    if (key && destDefn.value && !isEditMode.value) {
        destName.value = `My ${destDefn.value.name} Destination`
        resourceSelections.value = {}
        configData.value = {}
        nextStep()
    }
})

onMounted(() => {
    if (!isEditMode.value && props.initialTypeKey) {
        // Triggers the selection watcher above, which advances past the type step.
        selectedDestKey.value = props.initialTypeKey
    }
})

// ── Resource data caching (for x-fetch fields) ──────────────────

const resourceDataCache = ref<Record<string, Record<string, unknown>>>({})

watch(resourceSelections, async (selections) => {
    for (const [slotName, resourceId] of Object.entries(selections)) {
        if (!resourceId) {
            resourceDataCache.value = Object.fromEntries(
                Object.entries(resourceDataCache.value).filter(([k]) => k !== slotName),
            )
            continue
        }
        if (resourceDataCache.value[slotName]?._id === resourceId) continue
        try {
            const detail = await componentsStore.fetchOne(resourceId)
            resourceDataCache.value[slotName] = { ...detail.config, _id: resourceId }
        }
        catch { /* don't block */ }
    }
}, { deep: true })

const resourceContext = computed<Record<string, Record<string, unknown>>>(() => {
    const ctx: Record<string, Record<string, unknown>> = {}
    for (const [slotName, data] of Object.entries(resourceDataCache.value)) {
        const { _id, ...rest } = data
        ctx[slotName] = rest
    }
    return ctx
})

// ── Validation ───────────────────────────────────────────────────

const canProceed = computed(() => {
    const currentSlot = steps.value[activeStep.value]?.slot
    if (!currentSlot) return false

    if (currentSlot === 'destination') return !!selectedDestKey.value
    if (currentSlot === 'config') return configValid.value

    if (typeof currentSlot === 'string' && currentSlot.startsWith('resource-')) {
        const slotName = currentSlot.replace('resource-', '')
        return !!resourceSelections.value[slotName]
    }

    return false
})

// ── Submit ───────────────────────────────────────────────────────

async function submit() {
    if (!destDefn.value) return

    const resources: Record<string, string> = {}
    for (const [slotName, id] of Object.entries(resourceSelections.value)) {
        if (id) resources[slotName] = id
    }

    if (props.mode === 'collect') {
        emit('collected', {
            key: destDefn.value.key,
            name: destDefn.value.name,
            config: { ...configData.value },
            resources,
        })
        return
    }

    submitting.value = true
    try {
        const input: ComponentInput = {
            name: destName.value || destDefn.value.name,
            config: Object.keys(configData.value).length > 0 ? configData.value : undefined,
            relations: {
                resource: Object.entries(resources).map(([slot, dstId]) => ({ dst_id: dstId, slot })),
            },
        }

        if (isEditMode.value && props.destination) {
            await componentsStore.update(props.destination.id, input)
            toast.add({ title: `${destName.value} updated`, color: 'success' })
            emit('updated')
        }
        else {
            await componentsStore.create({ ...input, kind: 'destination', key: destDefn.value.key })
            toast.add({ title: `${destName.value} created`, color: 'success' })
            emit('created')
        }
    }
    catch {
        toast.add({ title: `Failed to ${isEditMode.value ? 'update' : 'create'} destination`, color: 'error' })
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

const title = computed(() => isEditMode.value ? 'Edit Destination' : 'New Destination')
const submitLabel = computed(() =>
    isLastStep.value
        ? (props.mode === 'collect'
            ? 'Confirm'
            : (isEditMode.value ? 'Save' : 'Create'))
        : 'Next',
)

defineExpose({ canProceed, hasPrev, isLastStep, submitting, submitLabel, title, next: handleNext, prev: prevStep })
</script>

<template>
    <UStepper v-model="activeStep"
              :items="displaySteps"
              linear
              disabled
              class="w-full">
        <!-- Step: Destination type (create only) -->
        <template v-if="!isEditMode"
                  #destination>
            <TypeSelect v-model="selectedDestKey"
                        :definitions="availableDefinitions" />
        </template>

        <!-- Dynamic resource steps -->
        <template v-for="rs in resourceSlots"
                  :key="rs.slotName"
                  #[`resource-${rs.slotName}`]>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard v-if="summaryCard"
                                 v-bind="summaryCard"
                                 @change="activeStep = 0" />

                <SourcesResourceStep v-if="rs.definition"
                                     :ref="(el: any) => { if (el) resourceStepRefs[rs.slotName] = el }"
                                     v-model="resourceSelections[rs.slotName]"
                                     :slot-name="rs.slotName"
                                     :definition="rs.definition"
                                     :resource-context="resourceContext"
                                     :silent="props.mode === 'collect'" />
            </div>
        </template>

        <!-- Config step -->
        <template v-if="hasConfig"
                  #config>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard v-if="summaryCard"
                                 v-bind="summaryCard"
                                 @change="activeStep = 0" />

                <UFormField label="Destination name">
                    <UInput v-model="destName"
                            placeholder="Destination name"
                            class="w-full" />
                </UFormField>

                <WizardRecap v-if="recapRows.length"
                             :rows="recapRows" />

                <USeparator label="Configuration" />

                <SchemaForm v-if="destDefn?.config_schema"
                            v-model:data="configData"
                            v-model:is-valid="configValid"
                            :schema="destDefn.config_schema"
                            :component-key="destDefn.key"
                            :resource-context="resourceContext" />
            </div>
        </template>
    </UStepper>
</template>
