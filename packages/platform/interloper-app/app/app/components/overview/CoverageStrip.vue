<script setup lang="ts">
import type { DayAggregate } from '~/composables/coverage'

const props = defineProps<{
    dates: string[]
    days: (DayAggregate | undefined)[]
    selected: string | null
}>()

const emit = defineEmits<{ select: [date: string] }>()

/** Past this many days the cells touch: a gap would eat most of each cell. */
const GAPLESS_FROM = 120

const colorMode = useColorMode()

const cells = computed(() => {
    const mode = colorMode.value === 'dark' ? 'dark' : 'light'
    return props.dates.map((date, i) => {
        const day = props.days[i]
        return {
            date,
            day,
            color: CELL[CELL_ORDER[cellStatus(day)]][mode],
        }
    })
})

/**
 * One tooltip per strip, anchored to the hovered cell: a strip can hold a year of cells. The last cell is kept
 * after the pointer leaves, so the tooltip fades out with its content.
 */
const hovered = shallowRef<{ element: HTMLElement, date: string, day: DayAggregate | undefined } | null>(null)
const open = ref(false)

function onPointerOver(event: PointerEvent) {
    const element = event.target as HTMLElement
    const cell = cells.value.find(c => c.date === element.dataset.date)
    if (cell) hovered.value = { element, date: cell.date, day: cell.day }
    open.value = !!cell
}

function onClick(event: MouseEvent) {
    const date = (event.target as HTMLElement).dataset.date
    if (date && cells.value.some(cell => cell.date === date && cell.day)) emit('select', date)
}
</script>

<template>
    <UTooltip :open="open"
              :reference="hovered?.element"
              :delay-duration="0"
              :content="{ side: 'top', sideOffset: 6 }"
              :ui="{ content: 'h-auto py-1.5 pointer-events-none' }">
        <div class="grid h-3.5 w-full cursor-pointer"
             :style="{ gridTemplateColumns: `repeat(${dates.length}, minmax(0, 1fr))`, gap: dates.length >= GAPLESS_FROM ? '0' : '1px' }"
             @click="onClick"
             @pointerover="onPointerOver"
             @pointerleave="open = false">
            <div v-for="cell in cells"
                 :key="cell.date"
                 :data-date="cell.date"
                 class="h-full rounded-[1px]"
                 :class="cell.date === selected ? 'relative z-10 outline-1 outline-offset-1 outline-(--ui-text-highlighted)' : ''"
                 :style="{ backgroundColor: cell.color }" />
        </div>
        <template #content>
            <OverviewCoverageDayBreakdown v-if="hovered"
                                          :date="hovered.date"
                                          :day="hovered.day" />
        </template>
    </UTooltip>
</template>
