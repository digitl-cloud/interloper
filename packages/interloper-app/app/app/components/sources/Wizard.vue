<script setup lang="ts">
/**
 * The source wizard: the definition-driven stepper plus the two steps only
 * sources have — asset selection (with cross-source dependency resolution)
 * and destination attachment. Both live here as extra steps; their state is
 * contributed to the payload via `extraInput` and the dependency wiring runs
 * post-save (it needs the saved children's ids).
 */
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import { allowedDestinationKeys } from '~/types/catalog'

const props = withDefaults(defineProps<{
    /** Pass an existing source to edit, or null to create. */
    source?: ComponentRecord | null
    /** Preselect this type and open directly on the next step (create mode). */
    initialTypeKey?: string
}>(), {
    source: null,
    initialTypeKey: undefined,
})

const emit = defineEmits<{
    created: []
    updated: []
}>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const toast = useToast()

const selectedAssetKeys = ref<string[]>(props.source ? props.source.children.map(a => a.key) : [])
const resolvedCrossDeps = ref<Record<string, string[]>>({})
const selectedDestinationIds = ref<string[]>(props.source ? relationIds(props.source, 'destinations') : [])

const sources = computed(() => componentsStore.byKind('source'))

/** Dynamic options context for SchemaForm's x-options-from fields. */
const optionsContext = computed(() => ({
    destinations: selectedDestinationIds.value.map((id) => {
        const dest = componentsStore.byId(id)
        const defn = dest ? catalogStore.catalog[dest.key] : undefined
        return { label: dest?.name ?? defn?.name ?? id, value: dest?.key ?? id }
    }),
}))

const extraSteps = [
    {
        name: 'assets',
        title: 'Assets',
        icon: 'i-lucide-layers',
        placement: 'start' as const,
        canProceed: () => selectedAssetKeys.value.length > 0,
        recap: () => `${selectedAssetKeys.value.length} selected`,
    },
    {
        name: 'destinations',
        title: 'Destination',
        icon: 'i-lucide-hard-drive',
        placement: 'end' as const,
        recap: () => {
            const names = selectedDestinationIds.value
                .map(id => componentsStore.byId(id)?.name)
                .filter(Boolean)
            return names.length ? names.join(', ') : 'None'
        },
    },
]

function extraInput() {
    return {
        children: selectedAssetKeys.value,
        relations: { destinations: selectedDestinationIds.value.map(id => ({ dst_id: id })) },
    }
}

/**
 * Cross-source deps are wired per child asset via the relations endpoint,
 * using the child ids from the save response.
 *
 * One leg at a time, and every failure is reported: the source itself is
 * already saved by the time this runs, so a leg that never binds leaves an
 * asset the user believes is wired unable to run. Re-submitting an
 * already-wired leg on edit is not a failure, the API returns the row it
 * holds.
 */
async function wireCrossDeps(saved: ComponentRecord) {
    const childIdByKey = new Map(saved.children.map(a => [a.key, a.id]))
    const failed: string[] = []
    for (const [key, upstreamIds] of Object.entries(resolvedCrossDeps.value)) {
        const [assetKey, name] = key.split('→')
        const childId = assetKey ? childIdByKey.get(assetKey) : undefined
        if (!childId || !name) continue
        for (const upstreamId of upstreamIds.filter(Boolean)) {
            try {
                await componentsStore.addRelation(childId, { name, dst_id: upstreamId })
            }
            catch {
                if (!failed.includes(`${assetKey}.${name}`)) failed.push(`${assetKey}.${name}`)
            }
        }
    }
    if (failed.length) {
        toast.add({
            title: 'Some dependencies were not wired',
            description: `${failed.join(', ')} could not be bound. Edit the source to wire them again.`,
            color: 'error',
        })
    }
}

const stepper = useTemplateRef('stepper')

defineExpose({
    canProceed: computed(() => stepper.value?.canProceed ?? false),
    hasPrev: computed(() => stepper.value?.hasPrev ?? false),
    isLastStep: computed(() => stepper.value?.isLastStep ?? false),
    submitting: computed(() => stepper.value?.submitting ?? false),
    submitLabel: computed(() => stepper.value?.submitLabel ?? 'Next'),
    title: computed(() => stepper.value?.title ?? 'New Source'),
    next: () => stepper.value?.next(),
    prev: () => stepper.value?.prev(),
})
</script>

<template>
    <WizardDefinitionStepper ref="stepper"
                             kind="source"
                             noun="Source"
                             :component="source"
                             :initial-type-key="initialTypeKey"
                             :definitions="catalogStore.sourceDefinitions"
                             resource-relation-steps
                             name-optional
                             :extra-steps="extraSteps"
                             :options-context="optionsContext"
                             :extra-input="extraInput"
                             :after-save="wireCrossDeps"
                             @created="emit('created')"
                             @updated="emit('updated')">
        <template #step-assets="{ definition }">
            <SourcesAssetSelect v-if="definition"
                                v-model:selected-keys="selectedAssetKeys"
                                v-model:resolved-deps="resolvedCrossDeps"
                                :source-defn="definition as SourceDefinition"
                                :all-sources="sources" />
        </template>

        <template #step-destinations="{ definition }">
            <SourcesDestinationStep v-model:selected-ids="selectedDestinationIds"
                                    :compatible-keys="definition ? allowedDestinationKeys(definition) : []" />
        </template>
    </WizardDefinitionStepper>
</template>
