<script setup lang="ts">
/** In-card asset list for a source's "List" expand mode. */
defineProps<{
    assets: GraphAssetEntry[]
}>()

const emit = defineEmits<{
    select: [entry: GraphAssetEntry]
}>()

function labelFor(e: GraphAssetEntry): string {
    return e.assetDefn?.name ?? e.asset.key
}

function iconFor(e: GraphAssetEntry): string {
    return e.assetDefn ? componentIcon(e.assetDefn.key) : 'i-lucide-box'
}
</script>

<template>
    <div class="nowheel py-1">
        <button v-for="e in assets"
                :key="e.asset.id"
                type="button"
                class="nodrag flex w-full items-center gap-2.5 px-4 py-2 text-left transition-colors hover:bg-elevated/60"
                @click.stop="emit('select', e)">
            <span class="size-1.5 shrink-0 rounded-full"
                  :class="statusDotClass(e.status?.state ?? 'idle')" />
            <UIcon :name="iconFor(e)"
                   class="size-4 shrink-0 text-muted" />
            <span class="min-w-0 flex-1 truncate text-xs">{{ labelFor(e) }}</span>
        </button>
    </div>
</template>
