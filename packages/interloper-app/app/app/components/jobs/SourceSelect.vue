<script setup lang="ts">
/**
 * Checkbox multi-select for choosing which sources to include in a job.
 */
import type { Source } from '~/types/source'

const selectedIds = defineModel<string[]>({ default: () => [] })

defineProps<{
    sources: Source[]
}>()

function sourceIcon(source: Source): string {
    return componentIcon(source.key)
}

function toggle(id: string) {
    const idx = selectedIds.value.indexOf(id)
    if (idx >= 0) selectedIds.value.splice(idx, 1)
    else selectedIds.value.push(id)
}

function selectAll(sources: Source[]) {
    selectedIds.value = sources.map(s => s.id)
}

function deselectAll() {
    selectedIds.value = []
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <div class="flex items-center justify-between">
            <span class="text-sm text-muted">
                {{ selectedIds.length }} of {{ sources.length }} sources selected
            </span>
            <div class="flex gap-2">
                <UButton size="xs"
                         variant="ghost"
                         label="Select all"
                         @click="selectAll(sources)" />
                <UButton size="xs"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectAll()" />
            </div>
        </div>

        <div v-if="sources.length === 0"
             class="flex flex-col items-center justify-center rounded-md p-6 gap-2">
            <span class="text-sm text-muted">No sources configured yet.</span>
        </div>

        <div v-else
             class="flex flex-col gap-1">
            <div v-for="source in sources"
                 :key="source.id"
                 class="flex items-center gap-3 px-3 py-2.5 rounded-md cursor-pointer bg-elevated/50 hover:bg-elevated transition-colors"
                 @click="toggle(source.id)">
                <UCheckbox :model-value="selectedIds.includes(source.id)"
                           @click.stop
                           @update:model-value="toggle(source.id)" />
                <UIcon :name="sourceIcon(source)"
                       class="size-5 shrink-0" />
                <div class="flex flex-col min-w-0">
                    <span class="text-sm font-medium">{{ source.name }}</span>
                    <span class="text-xs text-muted truncate">{{ source.key }}</span>
                </div>
                <UBadge color="neutral"
                        variant="subtle"
                        size="xs"
                        class="ml-auto">
                    {{ source.assets.length }} asset{{ source.assets.length !== 1 ? 's' : '' }}
                </UBadge>
            </div>
        </div>
    </div>
</template>
