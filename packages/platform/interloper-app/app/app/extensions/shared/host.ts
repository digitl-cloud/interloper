/**
 * `@interloper/host`: what an extension's UI uses from the app, under one module name.
 *
 * Every export is the app's own, so an extension's page reaches the same API
 * client, realtime connection, stores and toasts as the app, and the Nuxt UI
 * components here render with the app's theme (`app.config.ts`), which exists
 * only in the app's build. Each export is part of the contract extensions
 * build against: add to it deliberately.
 */

export { defineExtension, useExtension } from '~/composables/extensions'
export type { AssetUIContext, Extension, ExtensionAssetUI, ExtensionHost, ExtensionNavDestination, ExtensionNavPage, ExtensionRoute } from '~/composables/extensions'
export { useAssetRows } from '~/composables/assetRows'
export type { AssetRows } from '~/types/rows'

export { useApi, useOrgApi } from '~/composables/api'
export { useConfirm } from '~/composables/confirm'
export { useRealtimeSubscription } from '~/composables/realtime'
export { useToast } from '@nuxt/ui/composables/useToast'

export { useCatalogStore } from '~/stores/catalog'
export { useComponentsStore } from '~/stores/components'
export { useOrganisationStore } from '~/stores/organisation'
export { useRunsStore } from '~/stores/runs'
export { useUserStore } from '~/stores/user'

export { errorToast } from '~/utils/apiErrors'
export { componentIcon } from '~/utils/icons'

export { default as AppNavbar } from '~/components/ui/AppNavbar.vue'
export { default as AssetRowsTable } from '~/components/assets/RowsTable.vue'
export { default as AssetTableEditor } from '~/components/assets/TableEditor.vue'
export { default as CardHeader } from '~/components/ui/CardHeader.vue'
export { default as DataTable } from '~/components/ui/DataTable.vue'
export { default as LoadError } from '~/components/ui/LoadError.vue'
export { default as StatusBadge } from '~/components/ui/StatusBadge.vue'

export { default as UAlert } from '@nuxt/ui/components/Alert.vue'
export { default as UBadge } from '@nuxt/ui/components/Badge.vue'
export { default as UButton } from '@nuxt/ui/components/Button.vue'
export { default as UCard } from '@nuxt/ui/components/Card.vue'
export { default as UCheckbox } from '@nuxt/ui/components/Checkbox.vue'
export { default as UDashboardPanel } from '@nuxt/ui/components/DashboardPanel.vue'
export { default as UDashboardToolbar } from '@nuxt/ui/components/DashboardToolbar.vue'
export { default as UDropdownMenu } from '@nuxt/ui/components/DropdownMenu.vue'
export { default as UEmpty } from '@nuxt/ui/components/Empty.vue'
export { default as UForm } from '@nuxt/ui/components/Form.vue'
export { default as UFormField } from '@nuxt/ui/components/FormField.vue'
export { default as UIcon } from '@nuxt/ui/components/Icon.vue'
export { default as UInput } from '@nuxt/ui/components/Input.vue'
export { default as UModal } from '@nuxt/ui/components/Modal.vue'
export { default as UNavigationMenu } from '@nuxt/ui/components/NavigationMenu.vue'
export { default as UPageCard } from '@nuxt/ui/components/PageCard.vue'
export { default as USelect } from '@nuxt/ui/components/Select.vue'
export { default as USelectMenu } from '@nuxt/ui/components/SelectMenu.vue'
export { default as USeparator } from '@nuxt/ui/components/Separator.vue'
export { default as USkeleton } from '@nuxt/ui/components/Skeleton.vue'
export { default as USwitch } from '@nuxt/ui/components/Switch.vue'
export { default as UTable } from '@nuxt/ui/components/Table.vue'
export { default as UTabs } from '@nuxt/ui/components/Tabs.vue'
export { default as UTextarea } from '@nuxt/ui/components/Textarea.vue'
export { default as UTooltip } from '@nuxt/ui/components/Tooltip.vue'
