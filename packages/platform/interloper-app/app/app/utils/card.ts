/** UCard `ui` for a single-table page: the card fills the panel and its table scrolls inside it. */
export const FILL_CARD_UI = {
    root: 'flex-1 min-h-0 flex flex-col',
    body: 'flex-1 min-h-0 flex flex-col',
}

/** Fill card for a canvas (chart, graph) that draws edge to edge inside the card frame. */
export const CANVAS_CARD_UI = {
    ...FILL_CARD_UI,
    body: `${FILL_CARD_UI.body} p-0 sm:p-0`,
}
