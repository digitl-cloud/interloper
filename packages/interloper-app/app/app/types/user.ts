import type { Organisation } from './organisation'

export interface User {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
    organisation: Organisation | null
    last_organisation_id: string | null
}
