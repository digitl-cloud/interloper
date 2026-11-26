<script setup lang="ts">
/**
 * Choose the sources one asset reads: the same as its source, or only the ones
 * picked, which replace its source's upstream sources for this asset alone.
 */
import type { ComponentRecord } from '~/types/component'
import type { UpstreamGroup } from '~/utils/upstream'
import { UPSTREAM } from '~/utils/upstream'

const props = defineProps<{
    asset: ComponentRecord
    /** The asset's display name, for the title. */
    assetName: string
    source: ComponentRecord
    groups: UpstreamGroup[]
}>()

const open = defineModel<boolean>('open', { required: true })

const componentsStore = useComponentsStore()
const toast = useToast()

const draft = ref<string[] | undefined>()
const saving = ref(false)

watch(open, (opened) => {
    if (!opened) return
    const own = componentsStore.upstreamSources(props.asset.id)
    draft.value = own.length ? own : undefined
}, { immediate: true })

const sourceName = computed(() => props.source.name ?? props.source.key)
const candidateCount = computed(() => new Set(props.groups.flatMap(group => group.sources.map(source => source.id))).size)
const sourceUpstream = computed(() => componentsStore.upstreamSources(props.source.id))

const followDescription = computed(() => sourceUpstream.value.length
    ? `${sourceUpstream.value.length} sources, chosen on ${sourceName.value}`
    : `All ${candidateCount.value} sources of the organisation, as its source`)

const summary = computed(() => draft.value === undefined
    ? `Reads what ${sourceName.value} reads`
    : `Reads ${draft.value.length} of ${candidateCount.value} sources`)

async function save() {
    saving.value = true
    try {
        await componentsStore.update(props.asset.id, {
            relations: { [UPSTREAM]: (draft.value ?? []).map(id => ({ to_id: id })) },
        })
        componentsStore.refreshRelations()
        open.value = false
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't save the asset's upstream sources"))
    }
    finally {
        saving.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            :title="`Upstream · ${assetName}`"
            description="Which sources this asset reads.">
        <template #body>
            <SourcesUpstreamPicker v-model:selected-ids="draft"
                                   :groups="groups"
                                   follow-label="Same as its source"
                                   :follow-description="followDescription" />
        </template>
        <template #footer>
            <span class="me-auto text-xs text-muted">{{ summary }}</span>
            <UButton color="neutral"
                     variant="outline"
                     label="Cancel"
                     @click="open = false" />
            <UButton label="Save"
                     :loading="saving"
                     :disabled="draft !== undefined && draft.length === 0"
                     @click="save()" />
        </template>
    </UModal>
</template>
