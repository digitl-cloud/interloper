/** Sign-in provider name → its brand mark from the icon map, or the generic sign-in icon. */
export function useLoginProviderIcon() {
    const appConfig = useAppConfig()

    return (name: string): string => (appConfig.icons as Record<string, string>)[name] ?? appConfig.icons.signIn
}
