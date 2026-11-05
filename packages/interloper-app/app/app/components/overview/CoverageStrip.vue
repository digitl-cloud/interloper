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
            present: !!day,
            color: CELL[CELL_ORDER[cellStatus(day)]][mode],
            title: describeDay(date, day, ' · '),
        }
    })
})

/** One tooltip per strip, anchored to the hovered cell: a strip can hold a year of cells. */
const hovered = shallowRef<{ cell: HTMLElement, title: string } | null>(null)

function onPointerOver(event: PointerEvent) {
    const cell = event.target as HTMLElement
    const title = cells.value.find(c => c.date === cell.dataset.date)?.title
    hovered.value = title ? { cell, title } : null
}

function onClick(event: MouseEvent) {
    const date = (event.target as HTMLElement).dataset.date
    if (date && cells.value.some(cell => cell.date === date && cell.present)) emit('select', date)
}
</script>

<template>
    <UTooltip :open="!!hovered"
              :reference="hovered?.cell"
              :text="hovered?.title"
              :delay-duration="0"
              :content="{ side: 'top', sideOffset: 6 }">
        <div class="grid h-3.5 w-full cursor-pointer"
             :style="{ gridTemplateColumns: `repeat(${dates.length}, minmax(0, 1fr))`, gap: dates.length >= GAPLESS_FROM ? '0' : '1px' }"
             @click="onClick"
             @pointerover="onPointerOver"
             @pointerleave="hovered = null">
            <div v-for="cell in cells"
                 :key="cell.date"
                 :data-date="cell.date"
                 class="h-full rounded-[1px]"
                 :class="cell.date === selected ? 'relative z-10 outline-1 outline-offset-1 outline-(--ui-text-highlighted)' : ''"
                 :style="{ backgroundColor: cell.color }" />
        </div>
    </UTooltip>
</template>
