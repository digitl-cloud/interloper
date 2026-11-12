import type { StepperItem } from '@nuxt/ui'

/** Swap completed steps' icons for a check, based on the active index. */
export function useCheckedSteps(steps: MaybeRefOrGetter<StepperItem[]>, activeStep: Ref<number>) {
    const appConfig = useAppConfig()
    return computed<StepperItem[]>(() => toValue(steps).map((item, i) => ({
        ...item,
        icon: activeStep.value > i ? appConfig.ui.icons.check : item.icon,
    })))
}

/** Icon for a wizard resource-relation step or recap row. Call it where `useAppConfig` is available (setup, render). */
export function resourceRelationIcon(relationName: string): string {
    const { icons } = useAppConfig()
    return relationName === 'connection' ? icons.connection : icons.config
}

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
