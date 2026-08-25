<script setup lang="ts">
/**
 * Definition-driven create/edit wizard, piloted on jobs (ITLPR-121 phase 7).
 *
 * Steps derive from the catalog definition: one picker step per entry in
 * `relationSteps` (candidates from the relation's declared kinds), then a
 * Details step generated from `config_schema` via SchemaForm.
 *
 * Derived display and hand-built sections plug in through the `#details`
 * extension slot instead of forking the wizard: the slot receives the live
 * `configData` and `relations` selections plus an `extra` object whose
 * `config` is merged into the payload on submit and whose `valid` gates it.
 *
 * Container-agnostic: navigation state is exposed for WizardDrawer.
 */
import type { StepperItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'

const props = withDefaults(defineProps<{
    /** Component kind this wizard creates and edits (e.g. 'job'). */
    kind: string
    /** Catalog key of the class definition (e.g. 'cron_job'). */
    definitionKey: string
    /** Pass an existing component to edit, or null to create. */
    component: ComponentRecord | null
    /** Relation types rendered as picker steps, in order. Each requires at least one selection. */
    relationSteps?: string[]
    /** Config fields kept out of the generated form (owned by the `#details` slot). */
    exclude?: string[]
    /** Capitalized noun for titles and toasts (e.g. 'Job'). */
    noun: string
}>(), {
    relationSteps: () => [],
    exclude: () => [],
})

const emit = defineEmits<{
    created: []
    updated: []
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const definition = computed(() => catalogStore.catalog[props.definitionKey])
const schema = computed(() => definition.value?.config_schema)

// ── Form state ──────────────────────────────────────────────────
const name = ref('')
const configData = ref<Record<string, unknown>>({})
const configValid = ref(true)
const relationSelections = ref<Record<string, string[]>>(
    Object.fromEntries(props.relationSteps.map(type => [type, []])),
)
/** The `#details` slot's contribution: merged into config, gating submit. */
const extra = reactive<{ config: Record<string, unknown>, valid: boolean }>({ config: {}, valid: true })
const submitting = ref(false)

const isEditing = computed(() => !!props.component)

/**
 * Candidates for one relation step, from the definition's allowed kinds.
 * Owned assets run through their source, so only standalone assets are
 * offered — but selections a component already carries (e.g. set via the
 * API) stay listed so editing never silently drops them.
 */
function relationCandidates(type: string): ComponentRecord[] {
    const kinds = definition.value?.relations?.[type]?.kinds ?? []
    const natural = kinds.flatMap(kind =>
        componentsStore.byKind(kind).filter(c => c.kind !== 'asset' || c.parent_id === null),
    )
    const naturalIds = new Set(natural.map(c => c.id))
    const carried = (relationSelections.value[type] ?? [])
        .map(id => componentsStore.byId(id))
        .filter((c): c is ComponentRecord => !!c && !naturalIds.has(c.id))
    return [...natural, ...carried]
}

// ── Data fetching ────────────────────────────────────────────────

onMounted(async () => {
    const kinds = [...new Set(props.relationSteps.flatMap(type => definition.value?.relations?.[type]?.kinds ?? []))]
    await componentsStore.fetchAll(kinds)
    if (props.component) {
        name.value = props.component.name ?? ''
        // Seed only fields the schema declares: retired config keys are
        // dropped rather than resubmitted.
        const properties = new Set(Object.keys((schema.value?.properties as object | undefined) ?? {}))
        configData.value = Object.fromEntries(
            Object.entries(props.component.config ?? {})
                .filter(([key]) => properties.has(key) && !props.exclude.includes(key)),
        )
        for (const type of props.relationSteps) {
            relationSelections.value[type] = relationIds(props.component, type)
        }
    }
})

// ── Stepper ─────────────────────────────────────────────────────
const STEP_ICONS: Record<string, string> = {
    target: 'i-lucide-crosshair',
    watch: 'i-lucide-eye',
}

const steps = computed<StepperItem[]>(() => [
    ...props.relationSteps.map(type => ({
        title: type.charAt(0).toUpperCase() + type.slice(1) + 's',
        icon: STEP_ICONS[type] ?? 'i-lucide-link',
        slot: `relation-${type}` as const,
    })),
    { title: 'Details', icon: 'i-lucide-settings-2', slot: 'details' as const },
])

const { activeStep, hasPrev, isLastStep, next: nextStep, prev: prevStep } = useStepperFlow(computed(() => steps.value.length))

const displaySteps = useCheckedSteps(steps, activeStep)

/** Recap of the components picked on the relation steps. */
const recapRows = computed(() => props.relationSteps.map((type, i) => {
    const names = (relationSelections.value[type] ?? [])
        .map(id => componentsStore.byId(id)?.name)
        .filter(Boolean)
    return {
        icon: steps.value[i]!.icon!,
        label: steps.value[i]!.title!,
        value: names.length ? names.join(', ') : 'None',
    }
}))

// ── Validation ──────────────────────────────────────────────────
const detailsValid = computed(() => !!name.value.trim() && configValid.value && extra.valid)

const canProceed = computed(() => {
    const slot = steps.value[activeStep.value]?.slot ?? ''
    if (slot.startsWith('relation-')) {
        return (relationSelections.value[slot.slice('relation-'.length)] ?? []).length > 0
    }
    return detailsValid.value
})

// ── Submit ──────────────────────────────────────────────────────
async function submit() {
    submitting.value = true
    try {
        const input = {
            name: name.value.trim(),
            config: { ...configData.value, ...extra.config },
            relations: Object.fromEntries(
                props.relationSteps.map(type => [
                    type,
                    (relationSelections.value[type] ?? []).map(id => ({ dst_id: id })),
                ]),
            ),
        }

        if (props.component) {
            await componentsStore.update(props.component.id, input)
            toast.add({ title: `${props.noun} updated`, color: 'success' })
            emit('updated')
        }
        else {
            await componentsStore.create({ ...input, kind: props.kind, key: props.definitionKey })
            toast.add({ title: `${props.noun} created`, color: 'success' })
            emit('created')
        }
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
    <UStepper v-model="activeStep"
              :items="displaySteps"
              linear
              disabled
              class="w-full">
        <template v-for="type in relationSteps"
                  :key="type"
                  #[`relation-${type}`]>
            <WizardComponentSelect v-model="relationSelections[type]"
                                   :components="relationCandidates(type)"
                                   :noun="`${type}ed`" />
        </template>

        <template #details>
            <div class="flex flex-col gap-6">
                <WizardRecap v-if="recapRows.length"
                             :rows="recapRows" />

                <USeparator label="Configuration" />

                <UFormField :label="`${noun} name`"
                            required>
                    <UInput v-model="name"
                            :placeholder="`My ${noun.toLowerCase()}`"
                            class="w-full" />
                </UFormField>

                <SchemaForm v-if="schema"
                            v-model:data="configData"
                            v-model:is-valid="configValid"
                            :schema="schema"
                            :component-key="definitionKey"
                            :exclude="exclude" />

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
