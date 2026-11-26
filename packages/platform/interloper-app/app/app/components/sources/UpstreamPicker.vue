<script setup lang="ts">
/**
 * Pick the sources a source's assets, or one asset, read: all of them (`undefined`, following what is above:
 * the organisation, or the asset's source), or only the ones ticked.
 */
import { componentIcon } from '~/utils/icons'
import type { UpstreamGroup } from '~/utils/upstream'
import { upstreamGroupLabel } from '~/utils/upstream'

const props = defineProps<{
    groups: UpstreamGroup[]
    /** The first tab's words: "All sources", "Same as its source". */
    followLabel: string
    followDescription: string
}>()

const selectedIds = defineModel<string[] | undefined>('selectedIds', { required: true })

const appConfig = useAppConfig()
const search = ref('')

const mode = computed({
    get: () => (selectedIds.value === undefined ? 'follow' : 'only'),
    set: (value: string) => {
        selectedIds.value = value === 'follow' ? undefined : (selectedIds.value ?? [])
    },
})

const tabs = computed(() => [
    { value: 'follow', label: props.followLabel },
    { value: 'only', label: 'Only these' },
])

const description = computed(() => mode.value === 'follow'
    ? props.followDescription
    : 'Only the sources picked below. Sources added later are not read until you add them.')

const candidateIds = computed(() => [...new Set(props.groups.flatMap(group => group.sources.map(source => source.id)))])

const shown = computed(() => {
    const needle = search.value.trim().toLowerCase()
    return props.groups
        .map(group => ({
            ...group,
            sources: group.sources.filter(source => !needle
                || (source.name ?? source.key).toLowerCase().includes(needle)
                || (source.discriminator ?? '').toLowerCase().includes(needle)),
        }))
        .filter(group => group.sources.length)
})

function toggle(id: string) {
    const current = selectedIds.value ?? []
    selectedIds.value = current.includes(id) ? current.filter(held => held !== id) : [...current, id]
}
</script>

<template>
    <div class="flex flex-col gap-6">
        <div class="flex flex-col gap-2">
            <UTabs v-model="mode"
                   :items="tabs"
                   :content="false"
                   class="w-full" />
            <p class="text-xs text-muted">
                {{ description }}
            </p>
        </div>

        <template v-if="mode === 'only'">
            <UEmpty v-if="!candidateIds.length"
                    variant="subtle"
                    size="sm"
                    :icon="appConfig.icons.source"
                    title="No source to pick yet"
                    description="None of the organisation's sources holds an asset this one reads." />

            <template v-else>
                <div class="flex flex-col gap-3">
                    <FilterBar :active="0">
                        <template #search>
                            <UInput v-model="search"
                                    placeholder="Search sources…"
                                    :icon="appConfig.ui.icons.search"
                                    class="w-full sm:w-64" />
                        </template>
                    </FilterBar>

                    <div class="flex items-center justify-between">
                        <span class="text-sm text-muted tabular-nums">
                            {{ selectedIds?.length ?? 0 }} of {{ candidateIds.length }} sources selected
                        </span>
                        <div class="flex gap-2">
                            <UButton size="sm"
                                     color="neutral"
                                     variant="ghost"
                                     label="Select all"
                                     @click="selectedIds = [...candidateIds]" />
                            <UButton size="sm"
                                     color="neutral"
                                     variant="ghost"
                                     label="Deselect all"
                                     @click="selectedIds = []" />
                        </div>
                    </div>
                </div>

                <UEmpty v-if="!shown.length"
                        variant="naked"
                        size="sm"
                        :icon="appConfig.ui.icons.search"
                        title="No sources match this search" />

                <div v-for="group in shown"
                     :key="group.relations.join()"
                     class="flex flex-col gap-3">
                    <span v-if="groups.length > 1"
                          class="caption text-muted">
                        {{ upstreamGroupLabel(group) }}
                    </span>
                    <div class="flex flex-col gap-2">
                        <SelectionCard v-for="source in group.sources"
                                       :key="source.id"
                                       as="div"
                                       :selected="selectedIds?.includes(source.id)"
                                       :label="source.name || source.key"
                                       :icon="componentIcon(source.key, appConfig.icons.source)"
                                       @select="toggle(source.id)">
                            <template #leading>
                                <UCheckbox :model-value="selectedIds?.includes(source.id)"
                                           @click.stop
                                           @update:model-value="toggle(source.id)" />
                            </template>
                            <template v-if="source.discriminator"
                                      #trailing>
                                <UBadge color="neutral"
                                        size="sm"
                                        class="slashed-zero">
                                    {{ source.discriminator }}
                                </UBadge>
                            </template>
                        </SelectionCard>
                    </div>
                </div>
            </template>
        </template>
    </div>
</template>
