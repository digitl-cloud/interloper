<script setup lang="ts">
/**
 * Design catalog-grid card, used by the empty-state catalogs.
 *
 * Two variants, fully data-driven so every page renders the same structure:
 * - `rich`: icon tile + title/caption header, description body, pinned
 *   footer with info chips (left) and a "Set up →" action (right).
 * - `compact`: single row — icon tile, title/caption, trailing "+".
 */
withDefaults(defineProps<{
    icon: string
    title: string
    /** Mono uppercase caption under the title (e.g. tag or provider). */
    caption?: string
    /** Rich variant only: body text, clamped to two lines. */
    description?: string
    /** Rich variant only: bordered info chips in the footer. */
    chips?: { icon?: string, label: string }[]
    variant?: 'rich' | 'compact'
    /** Set false for static usage (e.g. selected-type summary): no hover/cursor. */
    interactive?: boolean
}>(), { variant: 'rich', caption: undefined, description: undefined, chips: () => [], interactive: true })
</script>

<template>
    <UCard v-if="variant === 'rich'"
           class="cursor-pointer transition hover:ring-primary/40 hover:shadow-md hover:-translate-y-0.5"
           :ui="{
               root: 'rounded-[13px] shadow-xs divide-y-0 flex flex-col',
               header: 'p-4 pb-0 sm:p-4 sm:pb-0',
               body: 'p-4 py-3 sm:p-4 sm:py-3 flex-1',
               footer: 'p-4 pt-0 sm:p-4 sm:pt-0',
           }">
        <template #header>
            <div class="flex items-center gap-3">
                <div class="size-[38px] shrink-0 rounded-[10px] border border-default flex items-center justify-center">
                    <UIcon :name="icon"
                           class="size-5" />
                </div>
                <div class="flex-1 min-w-0">
                    <div class="text-[14.5px] font-semibold text-highlighted truncate">{{ title }}</div>
                    <div v-if="caption"
                         class="font-mono text-[11px] uppercase tracking-[0.04em] text-dimmed mt-0.5 truncate">
                        {{ caption }}
                    </div>
                </div>
            </div>
        </template>

        <p v-if="description"
           class="text-[13px] text-muted leading-normal line-clamp-2">
            {{ description }}
        </p>

        <template #footer>
            <div class="flex items-center justify-between gap-2">
                <div class="flex items-center gap-2 min-w-0">
                    <span v-for="chip in chips"
                          :key="chip.label"
                          class="inline-flex items-center gap-1.5 border border-default rounded-md px-2 py-1 text-xs font-medium text-muted bg-(--ui-bg-band) truncate">
                        <UIcon v-if="chip.icon"
                               :name="chip.icon"
                               class="size-3 shrink-0" />
                        {{ chip.label }}
                    </span>
                </div>
                <span class="flex items-center gap-1.5 text-primary text-[13px] font-semibold shrink-0">
                    Set up
                    <UIcon name="i-lucide-arrow-right"
                           class="size-3" />
                </span>
            </div>
        </template>
    </UCard>

    <UCard v-else
           :class="interactive
               ? 'cursor-pointer transition hover:ring-primary/40 hover:shadow-md hover:-translate-y-0.5'
               : 'bg-(--ui-bg-band)'"
           :ui="{
               root: 'rounded-[13px] shadow-xs',
               body: 'p-3.5 px-4 sm:p-3.5 sm:px-4',
           }">
        <div class="flex items-center gap-3">
            <div class="size-10 shrink-0 rounded-[11px] border border-default bg-default flex items-center justify-center">
                <UIcon :name="icon"
                       class="size-6" />
            </div>
            <div class="flex-1 min-w-0">
                <div class="text-[14.5px] font-semibold text-highlighted truncate">{{ title }}</div>
                <div v-if="caption"
                     class="font-mono text-[11px] uppercase tracking-[0.05em] text-dimmed mt-0.5 truncate">
                    {{ caption }}
                </div>
            </div>
            <slot name="trailing">
                <UIcon v-if="interactive"
                       name="i-lucide-plus"
                       class="size-3.5 text-primary shrink-0" />
            </slot>
        </div>
    </UCard>
</template>
