<script setup lang="ts">
/**
 * Health banner for catalog drift, shared across the component pages and the
 * Catalog page. Surfaces removable drift of one kind (components whose key is
 * gone from the catalog; for sources, their drifted assets too) and offers a
 * one-click, confirmed cleanup. Self-contained: it reads state via useDrift
 * and performs the cleanup itself.
 */
const props = withDefaults(defineProps<{ kind?: string }>(), { kind: 'source' })

const componentsStore = useComponentsStore()
const appConfig = useAppConfig()
const toast = useToast()
const { confirm } = useConfirm()
const { missingOf, partialSources, missingAssetCount } = useDrift()

const cleaningUp = ref(false)

const isSource = computed(() => props.kind === 'source')
const missing = computed(() => missingOf(props.kind))
const assetCount = computed(() => (isSource.value ? missingAssetCount.value : 0))
const visible = computed(() => missing.value.length > 0 || assetCount.value > 0)
const singular = computed(() => props.kind.charAt(0).toUpperCase() + props.kind.slice(1))

function counted(n: number, kind: string): string {
    return `${n} ${n === 1 ? kind : kindLabel(kind).toLowerCase()}`
}

/** One-line summary of removable drift. */
const driftSummary = computed(() => {
    const parts: string[] = []
    if (missing.value.length) parts.push(counted(missing.value.length, props.kind))
    if (assetCount.value) parts.push(counted(assetCount.value, 'asset'))
    return parts.join(' and ')
})

const driftCount = computed(() => missing.value.length + assetCount.value)

async function handleCleanup() {
    const confirmed = await confirm({
        title: 'Clean up catalog drift',
        description: `This permanently removes ${driftSummary.value} that no longer ${driftCount.value === 1 ? 'exists' : 'exist'} in the catalog. This cannot be undone.`,
        confirmLabel: 'Remove',
    })
    if (!confirmed) return

    cleaningUp.value = true
    try {
        if (isSource.value) {
            // Prune drifted assets from still-valid sources, keeping the live ones.
            for (const source of partialSources.value) {
                const keep = source.children.filter(a => a.status !== 'missing').map(a => a.key)
                await componentsStore.update(source.id, { children: keep })
            }
        }
        const missingIds = missing.value.map(c => c.id)
        if (missingIds.length) await componentsStore.remove(missingIds)

        await componentsStore.fetchAll([props.kind])
        toast.add({ title: 'Catalog drift cleaned up', color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, singular.value) ?? errorToast(e, "Couldn't clean up the drift"))
    }
    finally {
        cleaningUp.value = false
    }
}
</script>

<template>
    <UAlert v-if="visible"
            color="error"
            variant="subtle"
            orientation="horizontal"
            :icon="appConfig.icons.drift"
            title="Catalog drift detected"
            :description="`${driftSummary} no longer ${driftCount === 1 ? 'exists' : 'exist'} in the catalog`"
            :actions="[{
                label: 'Clean up',
                icon: appConfig.icons.delete,
                color: 'error',
                variant: 'soft',
                loading: cleaningUp,
                onClick: handleCleanup,
            }]" />
</template>
