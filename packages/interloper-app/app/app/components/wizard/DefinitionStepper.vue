<script setup lang="ts">
/**
 * Definition-driven create/edit wizard for every component kind (ITLPR-126).
 *
 * Steps derive from the catalog definition:
 *   1. Type selection, when the kind has several classes and none is pinned
 *      via `definitionKey` (create mode only; `initialTypeKey` preselects).
 *   2. Page-injected extra steps with `placement: 'start'` (e.g. source assets).
 *   3. One picker step per `relationSteps` entry the selected class declares.
 *   4. One step per declared resource slot (when `resourceSlotSteps`).
 *   5. Extra steps with `placement: 'end'` (e.g. source destinations).
 *   6. Details: name + SchemaForm generated from `config_schema`, plus the
 *      `#details` extension slot whose `extra.config`/`extra.input` merge
 *      into the payload and whose `extra.valid` gates submit.
 *
 * Extra steps render through `#step-<name>` slots; their gating and recap
 * live in the entry's `canProceed`/`recap` callbacks so the page owns the
 * state they describe.
 *
 * Container-agnostic: navigation state is exposed for WizardDrawer.
 */
import type { FormError, StepperItem } from '@nuxt/ui'
import type { ComponentDefinition } from '~/types/catalog'
import { resourceSlots as definitionResourceSlots } from '~/types/catalog'
import type { ComponentRecord, RelationInput } from '~/types/component'
import { relationIds, resourceMap } from '~/types/component'

interface RelationStep {
    /** Relation type from the definition's vocabulary (e.g. 'target', 'watch'). */
    type: string
    /** Whether at least one selection is needed to proceed (default true). */
    required?: boolean
    /** Copy shown above the picker. */
    description?: string
    /** Offer only standalone assets (owned assets run through their source). */
    standaloneAssetsOnly?: boolean
}

interface ExtraStep {
    /** Slot suffix: content renders through `#step-<name>`. */
    name: string
    title: string
    icon: string
    /** 'start' = right after the type step, 'end' = right before Details. */
    placement: 'start' | 'end'
    /** Gate for proceeding past the step (default: always). */
    canProceed?: () => boolean
    /** Recap line for the Details step (omit for no row). */
    recap?: () => string
}

const props = withDefaults(defineProps<{
    /** Component kind this wizard creates and edits (e.g. 'job'). */
    kind: string
    /** Capitalized noun for titles and toasts (e.g. 'Job', 'Connection'). */
    noun: string
    /** Pass an existing component to edit, or null to create. */
    component: ComponentRecord | null
    /** Pin a single class (skips type selection, e.g. jobs' 'cron_job'). */
    definitionKey?: string
    /** Type-step choices; defaults to the kind's catalog definitions. */
    definitions?: ComponentDefinition[]
    /** Preselect this type and open directly on the next step (create mode). */
    initialTypeKey?: string
    relationSteps?: RelationStep[]
    /** Render one picker step per declared resource slot. */
    resourceSlotSteps?: boolean
    extraSteps?: ExtraStep[]
    /** Config fields kept out of the generated form (owned by the `#details` slot). */
    exclude?: string[]
    /** Allow an empty name (falls back to the type's display name). */
    nameOptional?: boolean
    /** Separator label above the generated form (e.g. 'Credentials'). */
    configLabel?: string
    /** Dynamic options for `x-options-from` fields, forwarded to SchemaForm. */
    optionsContext?: Record<string, { label: string, value: string }[]>
    /** Page-owned payload contributions (children, extra relations), read at submit. */
    extraInput?: () => { children?: string[], relations?: Record<string, RelationInput[]> }
    /** Post-save work needing the saved record (e.g. wiring asset dependencies). */
    afterSave?: (saved: ComponentRecord) => Promise<void>
}>(), {
    definitionKey: undefined,
    definitions: undefined,
    initialTypeKey: undefined,
    relationSteps: () => [],
    resourceSlotSteps: false,
    extraSteps: () => [],
    exclude: () => [],
    nameOptional: false,
    configLabel: 'Configuration',
    optionsContext: undefined,
    extraInput: undefined,
    afterSave: undefined,
})

const emit = defineEmits<{
    created: []
    updated: []
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const isEditing = computed(() => !!props.component)

// ── Type selection ───────────────────────────────────────────────
const selectedKey = ref(props.definitionKey ?? props.component?.key ?? '')

const typeChoices = computed(() => props.definitions ?? catalogStore.definitionsForKind(props.kind))

const hasTypeStep = computed(() => !props.definitionKey && !isEditing.value)

const definition = computed<ComponentDefinition | undefined>(() =>
    selectedKey.value ? catalogStore.catalog[selectedKey.value] ?? typeChoices.value.find(d => d.key === selectedKey.value) : undefined,
)

const schema = computed(() => definition.value?.config_schema)

const hasConfigFields = computed(() => {
    const properties = (schema.value?.properties as object | undefined) ?? {}
    return Object.keys(properties).some(key => key !== 'id')
})

/** Selected-type summary card shown on every post-type step. */
const summaryCard = computed(() => hasTypeStep.value || isEditing.value
    ? definition.value && {
            icon: componentIcon(definition.value.key),
            title: definition.value.name,
            caption: definition.value.tags?.[0] ?? props.noun,
            changeable: hasTypeStep.value,
        }
    : undefined)

// ── Form state ──────────────────────────────────────────────────
const name = ref('')
const configData = ref<Record<string, unknown>>({})
const configValid = ref(true)
const relationSelections = ref<Record<string, string[]>>(
    Object.fromEntries(props.relationSteps.map(step => [step.type, []])),
)
const resourceSelections = ref<Record<string, string>>({})
/** The `#details` slot's contribution: merged into the config, gating submit. */
const extra = reactive<{ config: Record<string, unknown>, valid: boolean }>({ config: {}, valid: true })
const submitting = ref(false)
const loadingEdit = ref(false)
/** A failed load of the edited component: the form must not pretend it is empty. */
const loadError = ref<Error | null>(null)

/** Editing a component whose stored config the server could not decrypt. */
const unreadableConfig = computed(() => props.component?.status === 'unreadable' && hasConfigFields.value)

/** Whether the schema form is in manual credential entry (vs OAuth sign-in). */
const manualCreds = ref(true)

const schemaForm = useTemplateRef('schemaForm')

/** Surface the connection check's static-validation errors under the form fields. */
function applyCheckErrors(errors: FormError[]) {
    schemaForm.value?.setErrors(errors)
}

// ── Relation steps ───────────────────────────────────────────────

/** The configured relation steps the selected class actually declares. */
const activeRelationSteps = computed(() =>
    props.relationSteps.filter(step => !!definition.value?.relations?.[step.type]),
)

/**
 * Candidates for one relation step, from the definition's allowed kinds.
 * Selections a component already carries (e.g. set via the API) stay
 * listed so editing never silently drops them.
 */
function relationCandidates(step: RelationStep): ComponentRecord[] {
    const kinds = definition.value?.relations?.[step.type]?.kinds ?? []
    const natural = kinds.flatMap(kind =>
        componentsStore.byKind(kind)
            .filter(c => !step.standaloneAssetsOnly || c.kind !== 'asset' || c.parent_id === null),
    )
    const naturalIds = new Set(natural.map(c => c.id))
    const carried = (relationSelections.value[step.type] ?? [])
        .map(id => componentsStore.byId(id))
        .filter((c): c is ComponentRecord => !!c && !naturalIds.has(c.id))
    return [...natural, ...carried]
}

// ── Resource slot steps ──────────────────────────────────────────

const resourceSlots = computed(() => {
    if (!props.resourceSlotSteps || !definition.value) return []
    return Object.entries(definitionResourceSlots(definition.value)).map(([slotName, resourceKey]) => ({
        slotName,
        resourceKey,
        definition: catalogStore.catalog[resourceKey],
    })).filter(rs => !!rs.definition)
})

// Cache the selected resources' config for SchemaForm x-fetch resolution.
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
        catch { /* don't block the form on a failed fetch */ }
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

// ── Data fetching ────────────────────────────────────────────────

onMounted(async () => {
    if (props.component) {
        loadingEdit.value = true
        loadError.value = null
        try {
            // Config always comes from the detail response: a secret kind's
            // list row carries at most the schema's x-public subset, which
            // must never seed (and then resubmit as) the whole form.
            const record = hasConfigFields.value
                ? await componentsStore.fetchOne(props.component.id)
                : props.component
            name.value = props.component.name ?? ''
            // Seed only fields the schema declares: retired config keys are
            // dropped rather than resubmitted.
            const properties = new Set(Object.keys((schema.value?.properties as object | undefined) ?? {}))
            configData.value = Object.fromEntries(
                Object.entries(record.config ?? {})
                    .filter(([key]) => properties.has(key) && !props.exclude.includes(key)),
            )
            for (const step of props.relationSteps) {
                relationSelections.value[step.type] = relationIds(props.component, step.type)
            }
            resourceSelections.value = resourceMap(props.component)
        }
        catch (e) {
            // Seeding half a form is worse than seeding none: say what failed
            // and let the user decide, rather than resubmitting blanks.
            loadError.value = e as Error
        }
        finally {
            loadingEdit.value = false
        }
    }
    else if (props.initialTypeKey) {
        // Triggers the selection watcher below, which advances past the type step.
        selectedKey.value = props.initialTypeKey
    }
    const kinds = [...new Set(props.relationSteps.flatMap(step => definition.value?.relations?.[step.type]?.kinds ?? []))]
    if (kinds.length) await componentsStore.fetchAll(kinds)
})

// When a type is selected (create mode only), reset the form and advance.
watch(selectedKey, (newKey, oldKey) => {
    if (newKey && newKey !== oldKey && !isEditing.value && hasTypeStep.value) {
        configData.value = {}
        configValid.value = false
        relationSelections.value = Object.fromEntries(props.relationSteps.map(step => [step.type, []]))
        resourceSelections.value = {}
        name.value = `My ${definition.value?.name ?? props.noun}`
        nextStep()
    }
})

// ── Derived name (sources: follow the discriminator until edited) ─

/** Label of the discriminator field's selected option, reported by the SchemaForm. */
const discriminatorLabel = ref<string | null>(null)

watch(discriminatorLabel, (label, old) => {
    if (!label) return
    const previous = old ?? `My ${definition.value?.name ?? props.noun}`
    if (!name.value || name.value === previous) name.value = label
})

// ── Stepper ─────────────────────────────────────────────────────
const RELATION_ICONS: Record<string, string> = {
    target: 'i-lucide-crosshair',
    watch: 'i-lucide-eye',
}

const steps = computed<StepperItem[]>(() => [
    ...(hasTypeStep.value ? [{ title: 'Type', icon: 'i-lucide-shapes', slot: 'type' as const }] : []),
    ...props.extraSteps.filter(s => s.placement === 'start')
        .map(s => ({ title: s.title, icon: s.icon, slot: `step-${s.name}` })),
    ...activeRelationSteps.value.map(step => ({
        title: step.type.charAt(0).toUpperCase() + step.type.slice(1) + (step.type.endsWith('ch') ? 'es' : 's'),
        icon: RELATION_ICONS[step.type] ?? 'i-lucide-link',
        slot: `relation-${step.type}`,
    })),
    ...resourceSlots.value.map(rs => ({
        title: rs.slotName.charAt(0).toUpperCase() + rs.slotName.slice(1),
        icon: resourceSlotIcon(rs.slotName),
        slot: `resource-${rs.slotName}`,
    })),
    ...props.extraSteps.filter(s => s.placement === 'end')
        .map(s => ({ title: s.title, icon: s.icon, slot: `step-${s.name}` })),
    { title: 'Details', icon: 'i-lucide-settings-2', slot: 'details' as const },
])

const { activeStep, hasPrev, isLastStep, next: nextStep, prev: prevStep } = useStepperFlow(computed(() => steps.value.length))

const displaySteps = useCheckedSteps(steps, activeStep)

/** Recap of what earlier steps chose, in step order. */
const recapRows = computed(() => {
    const rows: { icon: string, label: string, value: string }[] = []
    for (const item of steps.value) {
        const slot = String(item.slot)
        if (slot.startsWith('step-')) {
            const entry = props.extraSteps.find(s => s.name === slot.slice('step-'.length))
            if (entry?.recap) rows.push({ icon: item.icon!, label: item.title!, value: entry.recap() })
        }
        else if (slot.startsWith('relation-')) {
            const names = (relationSelections.value[slot.slice('relation-'.length)] ?? [])
                .map(id => componentsStore.byId(id)?.name)
                .filter(Boolean)
            rows.push({ icon: item.icon!, label: item.title!, value: names.length ? names.join(', ') : 'None' })
        }
        else if (slot.startsWith('resource-')) {
            const id = resourceSelections.value[slot.slice('resource-'.length)]
            rows.push({ icon: item.icon!, label: item.title!, value: id ? (componentsStore.byId(id)?.name ?? '—') : 'None' })
        }
    }
    return rows
})

// ── Validation ──────────────────────────────────────────────────
const detailsValid = computed(() =>
    (props.nameOptional || !!name.value.trim())
    && (!hasConfigFields.value || configValid.value)
    && extra.valid,
)

const canProceed = computed(() => {
    const slot = String(steps.value[activeStep.value]?.slot ?? '')
    if (slot === 'type') return !!selectedKey.value
    if (slot.startsWith('step-')) {
        const entry = props.extraSteps.find(s => s.name === slot.slice('step-'.length))
        return entry?.canProceed?.() ?? true
    }
    if (slot.startsWith('relation-')) {
        const type = slot.slice('relation-'.length)
        const step = props.relationSteps.find(s => s.type === type)
        return !(step?.required ?? true) || (relationSelections.value[type] ?? []).length > 0
    }
    if (slot.startsWith('resource-')) {
        return !!resourceSelections.value[slot.slice('resource-'.length)]
    }
    return detailsValid.value
})

// ── Submit ──────────────────────────────────────────────────────
async function submit() {
    if (!definition.value) return
    submitting.value = true
    try {
        const config = { ...configData.value, ...extra.config }
        const contributed = props.extraInput?.() ?? {}
        const relations: Record<string, RelationInput[]> = {
            ...Object.fromEntries(
                activeRelationSteps.value.map(step => [
                    step.type,
                    (relationSelections.value[step.type] ?? []).map(id => ({ dst_id: id })),
                ]),
            ),
            ...(props.resourceSlotSteps
                ? {
                        resource: Object.entries(resourceSelections.value)
                            .filter(([, id]) => !!id)
                            .map(([slot, id]) => ({ dst_id: id, slot })),
                    }
                : {}),
            ...contributed.relations,
        }
        const input = {
            name: name.value.trim() || definition.value.name,
            // Kinds without config fields never send one: an empty object
            // would clear a stored config on edit.
            ...(hasConfigFields.value || Object.keys(config).length ? { config } : {}),
            ...(contributed.children ? { children: contributed.children } : {}),
            relations,
        }

        let saved: ComponentRecord
        if (props.component) {
            saved = await componentsStore.update(props.component.id, input)
        }
        else {
            saved = await componentsStore.create({ ...input, kind: props.kind, key: definition.value.key })
        }
        await props.afterSave?.(saved)

        toast.add({ title: `${props.noun} ${isEditing.value ? 'updated' : 'created'}`, color: 'success' })
        if (isEditing.value) emit('updated')
        else emit('created')
    }
    catch (e) {
        toast.add(errorToast(e, `Failed to ${isEditing.value ? 'update' : 'create'} ${props.noun.toLowerCase()}`))
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

const title = computed(() => `${isEditing.value ? 'Edit' : 'New'} ${props.noun}`)
const submitLabel = computed(() => {
    if (!isLastStep.value) return 'Next'
    return isEditing.value ? `Save ${props.noun}` : `Create ${props.noun}`
})

defineExpose({ canProceed, hasPrev, isLastStep, submitting, submitLabel, title, next: handleNext, prev: prevStep })
</script>

<template>
    <div v-if="loadingEdit"
         class="flex items-center justify-center py-12">
        <UIcon name="i-lucide-loader-2"
               class="size-6 animate-spin text-muted" />
    </div>

    <UAlert v-else-if="loadError"
            color="error"
            icon="i-lucide-triangle-alert"
            :title="`Couldn't load this ${noun.toLowerCase()}`"
            :description="errorDetail(loadError) ?? GENERIC_ERROR" />

    <UStepper v-else
              v-model="activeStep"
              :items="displaySteps"
              linear
              disabled
              class="w-full">
        <template v-if="hasTypeStep"
                  #type>
            <WizardTypeSelect v-model="selectedKey"
                              :definitions="typeChoices" />
        </template>

        <template v-for="entry in extraSteps"
                  :key="entry.name"
                  #[`step-${entry.name}`]>
            <div class="flex flex-col gap-6">
                <WizardTypeSummaryCard v-if="summaryCard"
                                       v-bind="summaryCard"
                                       @change="activeStep = 0" />
                <slot :name="`step-${entry.name}`"
                      :definition="definition" />
            </div>
        </template>

        <template v-for="step in activeRelationSteps"
                  :key="step.type"
                  #[`relation-${step.type}`]>
            <div class="flex flex-col gap-4">
                <WizardTypeSummaryCard v-if="summaryCard"
                                       v-bind="summaryCard"
                                       @change="activeStep = 0" />
                <p v-if="step.description"
                   class="text-sm text-muted">
                    {{ step.description }}
                </p>
                <WizardComponentSelect v-model="relationSelections[step.type]"
                                       :components="relationCandidates(step)"
                                       :noun="`${step.type}ed`" />
            </div>
        </template>

        <template v-for="rs in resourceSlots"
                  :key="rs.slotName"
                  #[`resource-${rs.slotName}`]>
            <div class="flex flex-col gap-6">
                <WizardTypeSummaryCard v-if="summaryCard"
                                       v-bind="summaryCard"
                                       @change="activeStep = 0" />
                <SourcesResourceStep v-model="resourceSelections[rs.slotName]"
                                     :slot-name="rs.slotName"
                                     :definition="rs.definition!"
                                     :resource-context="resourceContext" />
            </div>
        </template>

        <template #details>
            <div class="flex flex-col gap-6">
                <WizardTypeSummaryCard v-if="summaryCard"
                                       v-bind="summaryCard"
                                       @change="activeStep = 0" />

                <UAlert v-if="unreadableConfig"
                        color="warning"
                        icon="i-lucide-lock"
                        :title="`This ${noun.toLowerCase()}'s stored config could not be read`"
                        description="The server's encryption key does not match the one it was saved with. The fields below start empty; saving replaces the unreadable config." />

                <WizardRecap v-if="recapRows.length"
                             :rows="recapRows" />

                <USeparator :label="configLabel" />

                <UFormField :label="`${noun} name`"
                            :required="!nameOptional">
                    <UInput v-model="name"
                            :placeholder="`My ${noun.toLowerCase()}`"
                            class="w-full" />
                </UFormField>

                <SchemaForm v-if="schema && hasConfigFields"
                            ref="schemaForm"
                            v-model:data="configData"
                            v-model:is-valid="configValid"
                            v-model:manual-mode="manualCreds"
                            v-model:discriminator-label="discriminatorLabel"
                            :schema="schema"
                            :component-key="selectedKey"
                            :exclude="exclude"
                            :resource-context="resourceContext"
                            :options-context="optionsContext" />
                <div v-else
                     class="text-sm text-muted italic">
                    No configuration required for this type.
                </div>

                <ResourcesConnectionCheck v-if="definition?.checkable && manualCreds"
                                          :component-key="selectedKey"
                                          :config="configData"
                                          manual
                                          @field-errors="applyCheckErrors" />

                <!-- Extension point: derived display and hand-built sections a
                     kind keeps outside the generated form. -->
                <slot name="details"
                      :config-data="configData"
                      :relations="relationSelections"
                      :extra="extra" />
            </div>
        </template>
    </UStepper>
</template>
