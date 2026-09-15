<script setup lang="ts">
/**
 * The dot-matrix "at work" indicator, ported from the Nuxt UI chat template's
 * own `ChatIndicator` so the agent reads the way their reference chat does.
 *
 * A 4x4 grid steps through a few patterns, one frame every 120ms.
 */
const SIZE = 4
const GAP = 2
const FRAME_MS = 120
const TOTAL_DOTS = SIZE * SIZE

const PATTERNS = [
    [[0], [1], [2], [3], [7], [11], [15], [14], [13], [12], [8], [4], [5], [6], [10], [9]],
    [[0, 4, 8, 12], [1, 5, 9, 13], [2, 6, 10, 14], [3, 7, 11, 15]],
    [[5, 6, 9, 10], [1, 4, 7, 8, 11, 14], [0, 3, 12, 15], [1, 4, 7, 8, 11, 14], [5, 6, 9, 10]],
    [[0], [1, 4], [2, 5, 8], [3, 6, 9, 12], [7, 10, 13], [11, 14], [15]],
]

const activeDots = ref<Set<number>>(new Set())

let patternIndex = 0
let stepIndex = 0
let timer: ReturnType<typeof setInterval> | undefined

function nextStep() {
    const pattern = PATTERNS[patternIndex]
    if (!pattern) return

    activeDots.value = new Set(pattern[stepIndex])
    stepIndex++

    if (stepIndex >= pattern.length) {
        stepIndex = 0
        patternIndex = (patternIndex + 1) % PATTERNS.length
    }
}

onMounted(() => {
    nextStep()
    timer = setInterval(nextStep, FRAME_MS)
})

onUnmounted(() => clearInterval(timer))
</script>

<template>
    <div class="shrink-0 grid size-4"
         :style="{ gridTemplateColumns: `repeat(${SIZE}, 1fr)`, gap: `${GAP}px` }">
        <span v-for="i in TOTAL_DOTS"
              :key="i"
              class="rounded-sm bg-current transition-opacity duration-100"
              :class="activeDots.has(i - 1) ? 'opacity-100' : 'opacity-20'" />
    </div>
</template>
