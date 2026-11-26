<script setup lang="ts">
/**
 * The source wizard: the definition-driven stepper plus the steps only
 * sources have, asset selection, the upstream sources a source reading other
 * sources follows, and destination attachment. They live here as extra steps;
 * their state is contributed to the payload via `extraInput`.
 */
import type { ComponentRecord } from '~/types/component'
import { relationIds } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import { allowedDestinationKeys, discriminatorField } from '~/types/catalog'
import { UPSTREAM, carriesUpstream, upstreamCandidates } from '~/utils/upstream'

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
const appConfig = useAppConfig()

const selectedAssetKeys = ref<string[]>(props.source ? props.source.children.map(a => a.key) : [])
const selectedDestinationIds = ref<string[]>(props.source ? relationIds(props.source, 'destinations') : [])

/** Dynamic options context for SchemaForm's x-options-from fields. */
const optionsContext = computed(() => ({
    destinations: selectedDestinationIds.value.map((id) => {
        const dest = componentsStore.byId(id)
        const defn = dest ? catalogStore.catalog[dest.key] : undefined
        return { label: dest?.name ?? defn?.name ?? id, value: dest?.key ?? id }
    }),
}))

const stepper = useTemplateRef('stepper')

const chosenDefinition = computed(() => {
    const key = props.source?.key ?? stepper.value?.definition?.key
    return key ? catalogStore.getSourceDefinition(key) : undefined
})

/** `undefined` follows every matching source; picked sources are the only ones read. */
const bound = props.source ? componentsStore.upstreamSources(props.source.id) : []
const selectedUpstreamIds = ref<string[] | undefined>(bound.length ? bound : undefined)

const upstreamGroups = computed(() => {
    const definition = chosenDefinition.value
    if (!definition || !carriesUpstream(definition)) return []
    return upstreamCandidates(definition, definition.key, componentsStore.byKind('source'), catalogStore.catalog, props.source?.id)
})
/** A source reading exactly one upstream source starts with that source's discriminator, which then names its tables. */
const suggestedConfig = computed(() => {
    const field = discriminatorField(chosenDefinition.value)
    const ids = selectedUpstreamIds.value
    const value = field && ids?.length === 1 ? componentsStore.byId(ids[0]!)?.discriminator : null
    return field && value ? { [field]: value } : {}
})
const upstreamCount = computed(() => new Set(upstreamGroups.value.flatMap(group => group.sources.map(source => source.id))).size)
const upstreamFollows = computed(() => {
    const keys = [...new Set(upstreamGroups.value.flatMap(group => group.sources.map(source => source.key)))]
    const type = keys.length === 1 ? (catalogStore.catalog[keys[0]!]?.name ?? keys[0]) : 'matching'
    return `All ${upstreamCount.value} ${type} sources of the organisation, and the ones added later.`
})

const extraSteps = computed(() => [
    {
        name: 'assets',
        title: 'Assets',
        icon: appConfig.icons.asset,
        placement: 'start' as const,
        canProceed: () => selectedAssetKeys.value.length > 0,
        recap: () => `${selectedAssetKeys.value.length} selected`,
    },
    ...(carriesUpstream(chosenDefinition.value)
        ? [{
                name: 'upstream',
                title: 'Upstream',
                icon: appConfig.icons.source,
                placement: 'end' as const,
                canProceed: () => selectedUpstreamIds.value === undefined || selectedUpstreamIds.value.length > 0,
                recap: () => selectedUpstreamIds.value === undefined
                    ? 'All sources'
                    : `${selectedUpstreamIds.value.length} of ${upstreamCount.value} sources`,
            }]
        : []),
    {
        name: 'destinations',
        title: 'Destination',
        icon: appConfig.icons.destination,
        placement: 'end' as const,
        recap: () => {
            const names = selectedDestinationIds.value
                .map(id => componentsStore.byId(id)?.name)
                .filter(Boolean)
            return names.length ? names.join(', ') : 'None'
        },
    },
])

/** Changed upstream sources change the edges every asset of the source follows. */
async function afterSave() {
    if (carriesUpstream(chosenDefinition.value)) componentsStore.refreshRelations()
}

/** Following sends no upstream source, which clears the rows any earlier pick left. */
function extraInput() {
    return {
        children: selectedAssetKeys.value,
        relations: {
            destinations: selectedDestinationIds.value.map(id => ({ to_id: id })),
            ...(carriesUpstream(chosenDefinition.value)
                ? { [UPSTREAM]: (selectedUpstreamIds.value ?? []).map(id => ({ to_id: id })) }
                : {}),
        },
    }
}

/**
 * A source without a time-partitioned asset has no history to start, so its form leaves `start` out.
 * An edited source's type is known before the stepper mounts, so its seeded config never loses the field.
 */
const exclude = computed(() => {
    const dated = chosenDefinition.value?.assets?.some(asset => ['time', 'snapshot'].includes(String(asset.partitioning?.kind)))
    return dated ? [] : ['start']
})

defineExpose({
    canProceed: computed(() => stepper.value?.canProceed ?? false),
    hasPrev: computed(() => stepper.value?.hasPrev ?? false),
    isLastStep: computed(() => stepper.value?.isLastStep ?? false),
    submitting: computed(() => stepper.value?.submitting ?? false),
    submitLabel: computed(() => stepper.value?.submitLabel ?? 'Next'),
    title: computed(() => stepper.value?.title ?? 'New source'),
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
                             name-optional
                             :extra-steps="extraSteps"
                             :exclude="exclude"
                             :options-context="optionsContext"
                             :extra-input="extraInput"
                             :after-save="afterSave"
                             :suggested-config="suggestedConfig"
                             @created="emit('created')"
                             @updated="emit('updated')">
        <template #step-assets="{ definition }">
            <SourcesAssetSelect v-if="definition"
                                v-model:selected-keys="selectedAssetKeys"
                                :source-defn="definition as SourceDefinition" />
        </template>

        <template #step-upstream>
            <div class="flex flex-col gap-6">
                <p class="text-sm text-muted">
                    Which sources do this source's assets read?
                </p>
                <SourcesUpstreamPicker v-model:selected-ids="selectedUpstreamIds"
                                       :groups="upstreamGroups"
                                       follow-label="All sources"
                                       :follow-description="upstreamFollows" />
            </div>
        </template>

        <template #step-destinations="{ definition }">
            <SourcesDestinationStep v-model:selected-ids="selectedDestinationIds"
                                    :compatible-keys="definition ? allowedDestinationKeys(definition) : []" />
        </template>
    </WizardDefinitionStepper>
</template>
