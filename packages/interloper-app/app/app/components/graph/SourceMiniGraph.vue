<script setup lang="ts">
interface MiniGraph {
    width: number
    height: number
    nodes: Array<{ entry: GraphAssetEntry; pos: { x: number; y: number } }>
    edges: Array<{ from: string; to: string }>
}

/** In-card mini dependency graph for a source's "Graph" expand mode. */
const props = defineProps<{
    miniGraph: MiniGraph
}>()

const emit = defineEmits<{
    select: [entry: GraphAssetEntry]
}>()

const NODE_W = 108
const NODE_H = 30

const posById = computed(() => {
    const map = new Map<string, { x: number; y: number }>()
    for (const n of props.miniGraph.nodes) map.set(n.entry.asset.id, n.pos)
    return map
})

function center(id: string): { x: number; y: number } {
    const p = posById.value.get(id) ?? { x: 0, y: 0 }
    return { x: p.x + NODE_W / 2, y: p.y + NODE_H / 2 }
}

function labelFor(e: GraphAssetEntry): string {
    return e.assetDefn?.name ?? e.asset.key
}
</script>

<template>
    <div class="nodrag nowheel relative"
         :style="{ width: `${miniGraph.width}px`, height: `${miniGraph.height}px` }">
        <svg class="pointer-events-none absolute inset-0 overflow-visible"
             :width="miniGraph.width"
             :height="miniGraph.height">
            <line v-for="(ed, i) in miniGraph.edges"
                  :key="i"
                  :x1="center(ed.from).x"
                  :y1="center(ed.from).y"
                  :x2="center(ed.to).x"
                  :y2="center(ed.to).y"
                  stroke="var(--ui-border-accented)"
                  stroke-width="1.5" />
        </svg>
        <button v-for="n in miniGraph.nodes"
                :key="n.entry.asset.id"
                type="button"
                class="absolute flex items-center gap-1.5 rounded-md border border-default bg-elevated px-2 transition-colors hover:border-primary"
                :style="{ left: `${n.pos.x}px`, top: `${n.pos.y}px`, width: `${NODE_W}px`, height: `${NODE_H}px` }"
                @click.stop="emit('select', n.entry)">
            <span class="size-1.5 shrink-0 rounded-full"
                  :class="statusDotClass(n.entry.status?.state ?? 'idle')" />
            <span class="min-w-0 flex-1 truncate text-left text-[11px]">{{ labelFor(n.entry) }}</span>
        </button>
    </div>
</template>
