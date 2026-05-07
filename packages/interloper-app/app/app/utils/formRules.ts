import cronstrue from 'cronstrue'

export const FormRules = {
    required: (v: string) => !!v || 'Required',
    number: (v: string) => !v || !isNaN(Number(v)) || 'Must be a number',
    cron: (v: string) => {
        if (!v) return true
        try {
            cronstrue.toString(v)
            return true
        } catch (e: any) {
            return `${e.replace('Error: ', '')}`
        }
    },
}