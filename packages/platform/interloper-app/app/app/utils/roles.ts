/** Badge colour of an organisation role: admins stand out, the other roles read as plain labels. */
export function roleColor(role: string): 'primary' | 'neutral' {
    return role === 'admin' ? 'primary' : 'neutral'
}
