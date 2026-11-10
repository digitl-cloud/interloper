<script setup lang="ts">
/**
 * Design catalog-grid card, used by the empty-state catalogs.
 *
 * Two variants, fully data-driven so every page renders the same structure:
 * - `rich`: icon tile + title/caption header, description body, pinned
 *   footer with info chips (left) and a "Set up →" action (right).
 * - `compact`: single row — icon tile, title/caption, trailing "+".
 */
import type { Maturity } from '~/types/catalog'
import { showsMaturity } from '~/types/catalog'

const props = withDefaults(defineProps<{
    icon: string
    title: string
    maturity?: Maturity
    /** Mono uppercase caption under the title (e.g. tag or provider). */
    caption?: string
    /** Rich variant only: body text, clamped to two lines. */
    description?: string
    /** Rich variant only: bordered info chips in the footer. */
    chips?: { icon?: string, label: string }[]
    variant?: 'rich' | 'compact'
    /** Set false for static usage (e.g. selected-type summary): no hover/cursor. */
    interactive?: boolean
}>(), { variant: 'rich', maturity: undefined, caption: undefined, description: undefined, chips: () => [], interactive: true })

const flagged = computed(() => showsMaturity(props.maturity))
</script>

<template>
    <UCard v-if="variant === 'rich'"
           class="relative cursor-pointer transition hover:ring-primary/40 hover:shadow-md hover:-translate-y-0.5"
           :ui="{
               root: 'rounded-lg shadow-xs divide-y-0 flex flex-col',
               header: 'p-4 pb-0 sm:p-4 sm:pb-0',
               body: 'p-4 py-3 sm:p-4 sm:py-3 flex-1',
               footer: 'p-4 pt-0 sm:p-4 sm:pt-0',
           }">
        <template #header>
            <div v-if="flagged"
                 class="absolute top-2.5 right-2.5">
                <MaturityBadge :maturity="maturity" />
            </div>
            <div class="flex items-center gap-3"
                 :class="{ 'pr-16': flagged }">
                <UIcon :name="icon"
                       class="size-6 shrink-0" />
                <div class="flex-1 min-w-0">
                    <div class="text-sm font-semibold text-highlighted truncate">{{ title }}</div>
                    <div v-if="caption"
                         class="font-mono text-xs uppercase tracking-[0.04em] text-dimmed mt-0.5 truncate">
                        {{ caption }}
                    </div>
                </div>
            </div>
        </template>

        <p v-if="description"
           class="text-sm text-muted leading-normal line-clamp-2">
            {{ description }}
        </p>

        <template #footer>
            <div class="flex items-center justify-between gap-2">
                <div class="flex items-center gap-2 min-w-0">
                    <span v-for="chip in chips"
                          :key="chip.label"
                          class="inline-flex items-center gap-1.5 border border-default rounded-md px-2 py-1 text-xs font-medium text-muted bg-elevated/50 truncate">
                        <UIcon v-if="chip.icon"
                               :name="chip.icon"
                               class="size-3 shrink-0" />
                        {{ chip.label }}
                    </span>
                </div>
                <span class="flex items-center gap-1.5 text-primary text-sm font-semibold shrink-0">
                    Set up
                    <UIcon name="i-lucide-arrow-right"
                           class="size-3" />
                </span>
            </div>
        </template>
    </UCard>

    <UCard v-else
           class="relative"
           :class="interactive
               ? 'cursor-pointer transition hover:ring-primary/40 hover:shadow-md hover:-translate-y-0.5'
               : ''"
           :ui="{
               root: 'rounded-lg shadow-xs',
               body: 'p-3.5 px-4 sm:p-3.5 sm:px-4',
           }">
        <div v-if="flagged"
             class="absolute top-1.5 right-1.5">
            <MaturityBadge :maturity="maturity" />
        </div>
        <div class="flex items-center gap-3">
            <UIcon :name="icon"
                   class="size-6 shrink-0" />
            <div class="flex-1 min-w-0">
                <div class="text-sm font-semibold text-highlighted truncate">{{ title }}</div>
                <div v-if="caption"
                     class="font-mono text-xs uppercase tracking-[0.05em] text-dimmed mt-0.5 truncate">
                    {{ caption }}
                </div>
            </div>
            <div class="shrink-0 flex"
                 :class="{ 'self-end': flagged }">
                <slot name="trailing">
                    <UIcon v-if="interactive"
                           name="i-lucide-plus"
                           class="size-3.5 text-primary shrink-0" />
                </slot>
            </div>
        </div>
    </UCard>
</template>
