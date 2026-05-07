export function useStepperFlow(stepsCount: MaybeRefOrGetter<number>) {
    const activeStep = ref(0)

    const hasPrev = computed(() => activeStep.value > 0)
    const hasNext = computed(() => activeStep.value < Math.max(0, toValue(stepsCount) - 1))
    const isLastStep = computed(() => !hasNext.value)

    function reset() {
        activeStep.value = 0
    }

    function next() {
        if (hasNext.value) activeStep.value += 1
    }

    function prev() {
        if (hasPrev.value) activeStep.value -= 1
    }

    return {
        activeStep,
        hasPrev,
        hasNext,
        isLastStep,
        reset,
        next,
        prev,
    }
}
