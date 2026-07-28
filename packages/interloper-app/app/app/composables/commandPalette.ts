import type { CommandPaletteGroup, CommandPaletteItem } from '@nuxt/ui'

export function useCommandPalette() {
    const userStore = useUserStore()
    const sections = useNavSections()
    const open = ref(false)

    defineShortcuts({
        meta_k: {
            usingInput: true,
            handler: () => {
                open.value = !open.value
            },
        },
    })

    const groups = computed<CommandPaletteGroup[]>(() => {
        const close = () => {
            open.value = false
        }
        const toItem = (page: NavPage): CommandPaletteItem => ({ ...page, onSelect: close })

        const settingsItems: CommandPaletteItem[] = [
            toItem({ label: 'Organization', icon: 'i-lucide-building-2', to: '/organization' }),
        ]
        if (userStore.isSuperAdmin)
            settingsItems.push(toItem({ label: 'Platform admin', icon: 'i-lucide-shield', to: '/admin' }))

        return [
            {
                id: 'pages',
                label: 'Pages',
                items: sections.value.flatMap(section => section.pages.map(toItem)),
            },
            {
                id: 'settings',
                label: 'Settings',
                items: settingsItems,
            },
        ]
    })

    return { open, groups }
}
