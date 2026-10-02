<script setup lang="ts">
/** Wizard summary of the selected type, with an optional "Change" action. */
import type { Maturity } from '~/types/catalog'

withDefaults(defineProps<{
    icon: string
    title: string
    maturity?: Maturity
    caption?: string
    /** Show the "Change" action (create mode only — editing can't change type). */
    changeable?: boolean
}>(), { maturity: undefined, caption: undefined, changeable: false })

const emit = defineEmits<{ change: [] }>()
</script>

<template>
    <CatalogCard variant="compact"
                 :interactive="false"
                 :icon="icon"
                 :title="title"
                 :maturity="maturity"
                 :caption="caption">
        <template v-if="changeable"
                  #trailing>
            <UButton label="Change"
                     variant="link"
                     size="sm"
                     class="shrink-0"
                     @click="emit('change')" />
        </template>
    </CatalogCard>
</template>
