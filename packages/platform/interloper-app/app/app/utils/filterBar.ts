import type { InjectionKey, Ref } from 'vue'

/** Set by `FilterBar`: whether its filters render stacked, in the phone's filter sheet. */
export const FILTER_BAR_STACKED: InjectionKey<Ref<boolean>> = Symbol('filter-bar-stacked')
