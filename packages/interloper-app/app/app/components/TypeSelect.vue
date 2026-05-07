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
                class="w-full" />

        <div v-if="filtered.length === 0"
             class="flex items-center justify-center rounded-md p-6 text-sm text-muted">
            No results match your search.
        </div>

        <template v-else>
            <div v-for="[tag, items] in groups"
                 :key="tag"
                 class="flex flex-col gap-2">
                <span v-if="tag"
                      class="text-sm font-medium text-muted capitalize tracking-wide">{{ tag }}</span>
                <div class="grid grid-cols-3 auto-rows-[7rem] gap-2">
                    <UCard v-for="defn in items"
                           :key="defn.key"
                           class="cursor-pointer text-center h-full"
                           :class="[
                               selectedKey === defn.key ? 'ring-primary ring-2' : '',
                           ]"
                           :ui="{ root: 'transition-colors hover:bg-elevated', body: 'flex flex-col items-center justify-center gap-4 p-3 sm:p-3 h-full' }"
                           @click="selectedKey = defn.key">
                        <UIcon :name="componentIcon(defn.key)"
                               class="size-8 shrink-0" />
                        <span class="text-sm font-medium leading-tight line-clamp-2">{{ defn.name }}</span>
                    </UCard>
                </div>
            </div>
        </template>
    </div>
</template>
