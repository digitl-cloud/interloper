<script setup lang="ts">
/**
 * Shared tile picker for selecting a component type (source, destination, resource).
 *
 * When definitions have `tags`, tiles are grouped by first tag.
 * Otherwise renders a flat grid.
 */
import type { ComponentDefinition } from '~/types/catalog'

type DefinitionItem = ComponentDefinition & { tags?: string[] }

const selectedKey = defineModel<string>({ default: '' })

const props = defineProps<{
    definitions: DefinitionItem[]
}>()

const search = ref('')

function matches(defn: DefinitionItem, query: string): boolean {
    const q = query.toLowerCase()
    return defn.name.toLowerCase().includes(q)
        || defn.key.toLowerCase().includes(q)
        || defn.description.toLowerCase().includes(q)
        || (defn.tags?.some(t => t.toLowerCase().includes(q)) ?? false)
}

const filtered = computed(() =>
    props.definitions.filter(d => !search.value || matches(d, search.value)),
)

/** Whether any definition has tags — controls grouped vs flat layout. */
const hasGroups = computed(() =>
    props.definitions.some(d => d.tags && d.tags.length > 0),
)

/** Group definitions by their first tag. Ungrouped items go under "Other". */
const groups = computed(() => {
    if (!hasGroups.value) return [['', filtered.value] as const]
    const map = new Map<string, DefinitionItem[]>()
    for (const defn of filtered.value) {
        const tag = defn.tags?.[0] ?? 'Other'
        if (!map.has(tag)) map.set(tag, [])
        map.get(tag)!.push(defn)
    }
    return [...map.entries()]
})
</script>

<template>
    <div class="space-y-4">
        <UInput v-model="search"
                placeholder="Search..."
                icon="i-lucide-search"
                variant="subtle"
                :ui="{ base: 'bg-muted' }"
                class="w-full" />

        <div v-if="filtered.length === 0"
             class="flex items-center justify-center rounded-md p-6 text-sm text-muted">
            No results match your search.
        </div>

        <template v-else>
            <div v-for="[tag, items] in groups"
                 :key="tag"
                 class="flex flex-col gap-3">
                <span v-if="tag"
                      class="eyebrow text-dimmed">{{ tag }}</span>
                <div class="grid grid-cols-2 gap-3">
                    <SelectionCard v-for="defn in items"
                                   :key="defn.key"
                                   :selected="selectedKey === defn.key"
                                   class="flex flex-col items-center gap-2.5 px-3.5 py-[18px]"
                                   @select="selectedKey = defn.key">
                        <div class="size-11 shrink-0 rounded-xl border border-default bg-default flex items-center justify-center">
                            <UIcon :name="componentIcon(defn.key)"
                                   class="size-[26px]" />
                        </div>
                        <span class="text-[13.5px] font-semibold text-highlighted text-center leading-tight line-clamp-2">{{ defn.name }}</span>
                    </SelectionCard>
                </div>
            </div>
        </template>
    </div>
</template>
