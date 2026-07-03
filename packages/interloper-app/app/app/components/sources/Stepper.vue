<script setup lang="ts">
/**
 * Multi-step form for creating or editing a source.
 *
 * Steps are dynamic based on the selected source definition:
 *   1. Source type (always, skip in edit mode)
 *   2. Assets (always)
 *   3..N. One step per resource slot (e.g. connection)
 *   N+1. Config (if source has config fields)
 *   N+2. Destination (select existing or create new)
 *
 * Container-agnostic: the parent wraps this in a UDrawer or any container.
 * Navigation state is exposed via defineExpose.
 */
import type { StepperItem } from '@nuxt/ui'
import type { Source } from '~/types/source'
import type { SourceDefinition } from '~/types/catalog'

const props = withDefaults(defineProps<{
    /** 'standalone' saves to API, 'collect' emits config without saving. */
    mode?: 'standalone' | 'collect'
    /** When set, stepper opens in edit mode with values pre-filled. */
    source?: Source | null
    /** Preselect this type and open directly on the next step (create mode). */
    initialTypeKey?: string
}>(), {
    mode: 'standalone',
    source: null,
    initialTypeKey: undefined,
})

const emit = defineEmits<{
    created: []
    updated: []
    collected: [config: {
        key: string
        name: string
        config: Record<string, any>
        resources: Record<string, string>
        assetKeys: string[]
        resolvedDeps: Record<string, string>
    }]
}>()

const isEditMode = computed(() => !!props.source)

const catalogStore = useCatalogStore()
const sourcesStore = useSourcesStore()
const resourcesStore = useResourcesStore()
const destinationsStore = useDestinationsStore()
const toast = useToast()

// ── State ────────────────────────────────────────────────────────
const selectedSourceKey = ref('')
const sourceName = ref('')
const selectedAssetKeys = ref<string[]>([])
const resolvedCrossDeps = ref<Record<string, string>>({})
const resourceSelections = ref<Record<string, string>>({})
const configData = ref<Record<string, any>>({})
const configValid = ref(true)
const selectedDestinationIds = ref<string[]>([])
const submitting = ref(false)

/** Dynamic options context for SchemaForm's x-options-from fields. */
const optionsContext = computed(() => ({
    destinations: selectedDestinationIds.value.map((id) => {
        const dest = destinationsStore.findById(id)
        const defn = dest ? catalogStore.catalog[dest.key] : undefined
        return { label: dest?.name ?? defn?.name ?? id, value: dest?.key ?? id }
    }),
}))

/** Template refs for ResourceStep components, keyed by slot name. */
const resourceStepRefs = ref<Record<string, any>>({})

// ── Edit mode pre-fill ──────────────────────────────────────────

if (props.source) {
    selectedSourceKey.value = props.source.key
    sourceName.value = props.source.name
    selectedAssetKeys.value = props.source.assets.map(a => a.key)
    configData.value = { ...(props.source.config || {}) }
    resourceSelections.value = { ...props.source.resources }
    selectedDestinationIds.value = props.source.destinations.map(d => d.id)
}

// ── Derived ──────────────────────────────────────────────────────

const sourceDefn = computed<SourceDefinition | undefined>(() =>
    selectedSourceKey.value ? catalogStore.getSourceDefinition(selectedSourceKey.value) : undefined,
)

/** Resource slots (connections only — configs are now source fields). */
const resourceSlots = computed(() => {
    if (!sourceDefn.value) return []
    return Object.entries(sourceDefn.value.resources).map(([slotName, resourceKey]) => ({
        slotName,
        resourceKey,
        definition: catalogStore.catalog[resourceKey],
    }))
})

/** Whether the source has user-facing config fields. */
const hasConfig = computed(() => {
    const schema = sourceDefn.value?.config_schema
    return schema && Object.keys(schema.properties || {}).length > 0
})

/** Dynamic stepper items. */
const steps = computed<StepperItem[]>(() => {
    const items: StepperItem[] = []

    // Step 1: Source type (skip in edit mode)
    if (!isEditMode.value) {
        items.push({ title: 'Source', icon: 'i-lucide-plug', slot: 'source' as const })
    }

    // Step 2: Assets
    items.push({ title: 'Assets', icon: 'i-lucide-layers', slot: 'assets' as const })

    // Resource steps (connections)
    for (const rs of resourceSlots.value) {
        if (!rs.definition) continue
        const kind = rs.slotName.charAt(0).toUpperCase() + rs.slotName.slice(1)
        items.push({
            title: kind,
            icon: resourceSlotIcon(rs.slotName),
            slot: `resource-${rs.slotName}` as any,
        })
    }

    // Destination step
    items.push({ title: 'Destination', icon: 'i-lucide-hard-drive', slot: 'destination' as const })

    // Config step (always last — at minimum contains the name field)
    items.push({ title: 'Config', icon: 'i-lucide-settings-2', slot: 'config' as const })

    return items
})

const totalSteps = computed(() => steps.value.length)
const { activeStep, hasPrev, isLastStep, reset: resetStepper, next: nextStep, prev: prevStep } = useStepperFlow(totalSteps)

const displaySteps = useCheckedSteps(steps, activeStep)

/** Selected-type summary card shown on every post-type step. */
const summaryCard = computed(() => sourceDefn.value && {
    icon: componentIcon(sourceDefn.value.key),
    title: sourceDefn.value.name,
    caption: sourceDefn.value.tags?.[0] ?? 'Source',
    changeable: !isEditMode.value,
})

/** Recap rows for the final step — what was chosen along the way. */
const recapRows = computed(() => {
    const rows = [{
        icon: 'i-lucide-layers',
        label: 'Assets',
        value: `${selectedAssetKeys.value.length} selected`,
    }]
    for (const rs of resourceSlots.value) {
        const id = resourceSelections.value[rs.slotName]
        rows.push({
            icon: resourceSlotIcon(rs.slotName),
            label: rs.slotName.charAt(0).toUpperCase() + rs.slotName.slice(1),
            value: id ? (resourcesStore.findById(id)?.name ?? '—') : 'None',
        })
    }
    const destNames = selectedDestinationIds.value
        .map(id => destinationsStore.findById(id)?.name)
        .filter(Boolean)
    rows.push({
        icon: 'i-lucide-hard-drive',
        label: 'Destinations',
        value: destNames.length ? destNames.join(', ') : 'None',
    })
    return rows
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
            const detail = await resourcesStore.fetchOne(resourceId)
            resourceDataCache.value[slotName] = { ...detail.data, _id: resourceId }
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

// ── Auto-advance on source selection ────────────────────────────

watch(selectedSourceKey, (key) => {
    if (key && sourceDefn.value && !isEditMode.value) {
        sourceName.value = `My ${sourceDefn.value.name} Source`
        selectedAssetKeys.value = sourceDefn.value.assets.map(a => a.key)
        resourceSelections.value = {}
        configData.value = {}
        nextStep()
    }
})

onMounted(() => {
    if (!isEditMode.value && props.initialTypeKey) {
        // Triggers the selection watcher above, which advances past the type step.
        selectedSourceKey.value = props.initialTypeKey
    }
})

// ── Validation ───────────────────────────────────────────────────

const canProceed = computed(() => {
    const currentSlot = steps.value[activeStep.value]?.slot
    if (!currentSlot) return false

    if (currentSlot === 'source') return !!selectedSourceKey.value
    if (currentSlot === 'assets') return selectedAssetKeys.value.length > 0
    if (currentSlot === 'config') return configValid.value
    if (currentSlot === 'destination') return true // Optional — can proceed without one
    // Resource steps
    if (typeof currentSlot === 'string' && currentSlot.startsWith('resource-')) {
        const slotName = currentSlot.replace('resource-', '')
        return !!resourceSelections.value[slotName]
    }

    return false
})

// ── Submit ───────────────────────────────────────────────────────

async function submit() {
    if (!sourceDefn.value) return

    const resources: Record<string, string> = {}
    for (const [slotName, id] of Object.entries(resourceSelections.value)) {
        if (id) resources[slotName] = id
    }

    if (props.mode === 'collect') {
        emit('collected', {
            key: sourceDefn.value.key,
            name: sourceDefn.value.name,
            config: { ...configData.value },
            resources,
            assetKeys: selectedAssetKeys.value,
            resolvedDeps: { ...resolvedCrossDeps.value },
        })
        return
    }

    submitting.value = true
    try {
        // Convert resolvedCrossDeps from "assetKey→paramName" → upstreamId
        // to API format: { asset_key: { param_name: upstream_asset_id } }
        const crossDeps: Record<string, Record<string, string>> = {}
        for (const [key, upstreamId] of Object.entries(resolvedCrossDeps.value)) {
            const [assetKey, paramName] = key.split('→')
            if (assetKey && paramName && upstreamId) {
                if (!crossDeps[assetKey]) crossDeps[assetKey] = {}
                crossDeps[assetKey][paramName] = upstreamId
            }
        }

        const input = {
            key: sourceDefn.value.key,
            name: sourceName.value || sourceDefn.value.name,
            config: Object.keys(configData.value).length > 0 ? configData.value : undefined,
            resources: Object.keys(resources).length > 0 ? resources : undefined,
            asset_keys: selectedAssetKeys.value,
            destination_ids: selectedDestinationIds.value.length > 0 ? selectedDestinationIds.value : undefined,
            cross_deps: Object.keys(crossDeps).length > 0 ? crossDeps : undefined,
        }

        if (isEditMode.value && props.source) {
            await sourcesStore.update(props.source.id, input)
            toast.add({ title: `${sourceName.value} updated`, color: 'success' })
            emit('updated')
        }
        else {
            await sourcesStore.create(input)
            toast.add({ title: `${sourceName.value} created`, color: 'success' })
            emit('created')
        }
    }
    catch {
        toast.add({ title: `Failed to ${isEditMode.value ? 'update' : 'create'} source`, color: 'error' })
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

const title = computed(() => isEditMode.value ? 'Edit Source' : 'New Source')
const submitLabel = computed(() =>
    isLastStep.value
        ? (props.mode === 'collect' ? 'Confirm' : (isEditMode.value ? 'Save' : 'Create'))
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
        <!-- Step: Source type (create only) -->
        <template v-if="!isEditMode"
                  #source>
            <TypeSelect v-model="selectedSourceKey"
                        :definitions="catalogStore.sourceDefinitions" />
        </template>

        <!-- Step: Assets -->
        <template #assets>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard v-if="summaryCard"
                                 v-bind="summaryCard"
                                 @change="activeStep = 0" />
                <SourcesAssetSelect v-if="sourceDefn"
                                    v-model:selected-keys="selectedAssetKeys"
                                    v-model:resolved-deps="resolvedCrossDeps"
                                    :source-defn="sourceDefn"
                                    :all-sources="sourcesStore.sources" />
            </div>
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

        <!-- Step: Config -->
        <template #config>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard v-if="summaryCard"
                                 v-bind="summaryCard"
                                 @change="activeStep = 0" />

                <UFormField label="Source name">
                    <UInput v-model="sourceName"
                            placeholder="Source name"
                            class="w-full" />
                </UFormField>

                <WizardRecap :rows="recapRows" />

                <USeparator label="Configuration" />

                <SchemaForm v-if="sourceDefn?.config_schema"
                            v-model:data="configData"
                            v-model:is-valid="configValid"
                            :schema="sourceDefn.config_schema"
                            :component-key="sourceDefn.key"
                            :resource-context="resourceContext"
                            :options-context="optionsContext" />
            </div>
        </template>

        <!-- Step: Destination -->
        <template #destination>
            <div class="flex flex-col gap-6">
                <TypeSummaryCard v-if="summaryCard"
                                 v-bind="summaryCard"
                                 @change="activeStep = 0" />
                <SourcesDestinationStep v-model:selected-ids="selectedDestinationIds"
                                        :compatible-keys="sourceDefn?.destinations ?? []" />
            </div>
        </template>
    </UStepper>
</template>
