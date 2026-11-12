<script setup lang="ts">
/**
 * A catalog entry as a card, used by the empty-state catalogs and the
 * wizard's type summary. Both variants open on the entry's icon, its title
 * (in the card title's style) over a caption (a tag or provider), and its
 * maturity badge, in the card's header:
 * - `rich`: the description as the body, and a footer with neutral info
 *   chips on the left and a "Set up" link on the right.
 * - `compact`: the header alone, ending in a trailing "+" (or the `trailing` slot).
 * The whole card takes the host's click.
 */
import type { Maturity } from '~/types/catalog'
import { showsMaturity } from '~/types/catalog'

const props = withDefaults(defineProps<{
    icon: string
    title: string
    maturity?: Maturity
    /** Caption under the title (e.g. tag or provider). */
    caption?: string
    /** Rich variant only: body text, clamped to two lines. */
    description?: string
    /** Rich variant only: info chips in the footer. */
    chips?: { icon?: string, label: string }[]
    variant?: 'rich' | 'compact'
    /** Set false for static usage (e.g. selected-type summary): no hover/cursor. */
    interactive?: boolean
}>(), { variant: 'rich', maturity: undefined, caption: undefined, description: undefined, chips: () => [], interactive: true })

const appConfig = useAppConfig()
const flagged = computed(() => showsMaturity(props.maturity))
</script>

<template>
    <UCard :class="interactive && 'cursor-pointer transition hover:ring-primary/50'"
           :ui="{ root: 'flex flex-col', body: 'flex-1' }">
        <template #header>
            <div class="flex items-center gap-3">
                <UIcon :name="icon"
                       class="size-5 shrink-0" />
                <div class="min-w-0 flex-1">
                    <div class="truncate"
                         :class="appConfig.ui.card.slots.title">{{ title }}</div>
                    <div v-if="caption"
                         class="caption truncate text-muted">{{ caption }}</div>
                </div>
                <MaturityBadge v-if="flagged"
                               :maturity="maturity" />
                <slot v-if="variant === 'compact'"
                      name="trailing">
                    <UIcon v-if="interactive"
                           :name="appConfig.ui.icons.plus"
                           class="size-4 shrink-0 text-primary" />
                </slot>
            </div>
        </template>

        <template v-if="variant === 'rich' && description"
                  #default>
            <p class="line-clamp-2 text-sm text-muted">
                {{ description }}
            </p>
        </template>

        <template v-if="variant === 'rich'"
                  #footer>
            <div class="flex items-center justify-between gap-2">
                <div class="flex min-w-0 items-center gap-2">
                    <UBadge v-for="chip in chips"
                            :key="chip.label"
                            color="neutral"
                            :icon="chip.icon"
                            :label="chip.label" />
                </div>
                <UButton label="Set up"
                         variant="link"
                         size="sm"
                         :trailing-icon="appConfig.ui.icons.arrowRight"
                         class="shrink-0" />
            </div>
        </template>
    </UCard>
</template>
