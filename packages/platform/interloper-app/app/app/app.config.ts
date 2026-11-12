export default defineAppConfig({
  ui: {
    colors: {
      primary: 'blue',
      success: 'emerald',
      warning: 'amber',
      neutral: 'neutral'
    },
    badge: {
      // `soft` = tinted fill, no ring: statuses, tags and counts all ride this
      // default. EntityBadge is the one exception, opting into `outline` so
      // object references read as references. Counts line up in tabular figures.
      slots: {
        base: 'tabular-nums'
      },
      defaultVariants: {
        size: 'md',
        variant: 'soft'
      }
    },
    button: {
      defaultVariants: {
        size: 'lg',
        variant: 'solid'
      }
    },
    card: {
      // Section cards: the card tone, no rules between header, body and
      // footer. CardHeader reads `title` / `description` from here so a card
      // with controls reads the same as one using the title props.
      slots: {
        // overflow-hidden zeroes a flex item's auto min-height, so without this
        // cards squash to fit a page body instead of letting it scroll.
        root: 'shrink-0',
        title: 'text-highlighted font-semibold',
        description: 'mt-1 text-muted text-sm',
        body: '[[data-slot=header]+&]:pt-0 sm:[[data-slot=header]+&]:pt-0',
        footer: '[[data-slot=body]+&]:pt-0'
      },
      variants: {
        variant: {
          outline: {
            root: 'bg-muted ring ring-default divide-y-0'
          }
        }
      },
      defaultVariants: {
        variant: 'outline'
      }
    },
    // A page-level empty state sits on the panel the way a card does.
    empty: {
      variants: {
        variant: {
          outline: {
            root: 'bg-muted'
          }
        }
      }
    },
    dashboardSidebar: {
      slots: {
        root: 'bg-muted',
        // The mobile slideover renders through `content`, not `root`.
        content: 'bg-muted',
        footer: 'border-t border-default'
      }
    },
    // The agent panel slides in below the navbar: the toolbar and the body make room for it
    // (the panel sizes `agent-room` from its own stylesheet), the navbar keeps the full width.
    dashboardPanel: {
      slots: {
        body: 'agent-room transition-[margin]'
      }
    },
    // A page's toolbar (its sub-page tabs, a detail page's actions) is the first row of the page's
    // content, not a band of its own: no rule, and as far below the navbar as the content sits.
    dashboardToolbar: {
      slots: {
        root: 'agent-room transition-[margin] min-h-0 overflow-visible border-b-0 pt-4 sm:pt-6'
      }
    },
    // The navbar's search field reads as a filled control, not an outlined button.
    dashboardSearchButton: {
      slots: {
        base: 'bg-elevated ring-default'
      }
    },
    navigationMenu: {
      slots: {
        label: 'caption text-muted'
      },
      variants: {
        orientation: {
          vertical: {
            link: 'py-2'
          }
        }
      },
      // Sidebar entries read bright. The active pill keeps Nuxt UI's bg-elevated,
      // ringed so it still shows on the near-identical light sidebar tone.
      compoundVariants: [
        {
          orientation: 'vertical',
          active: false,
          class: {
            link: 'text-default',
            linkLeadingIcon: 'text-default'
          }
        },
        {
          orientation: 'vertical',
          variant: 'pill',
          active: true,
          highlight: false,
          class: {
            link: 'before:ring before:ring-default'
          }
        },
        // Horizontal pills (a page's sub-page tabs) are a segmented control, the same as the
        // pill UTabs below: a card-toned track, the active view a raised chip.
        {
          orientation: 'horizontal',
          variant: 'pill',
          class: {
            list: 'gap-1 rounded-lg bg-muted p-1 ring ring-default',
            item: 'py-0',
            link: 'px-3 py-1',
            linkLeadingIcon: 'size-4'
          }
        },
        {
          orientation: 'horizontal',
          variant: 'pill',
          active: true,
          class: {
            link: 'before:bg-default before:shadow-xs dark:before:bg-inverted dark:text-inverted',
            linkLeadingIcon: 'dark:text-inverted'
          }
        }
      ]
    },
    // Horizontal rows (settings) stack on narrow screens.
    formField: {
      variants: {
        orientation: {
          horizontal: {
            root: 'max-sm:flex-col'
          }
        }
      }
    },
    input: {
      defaultVariants: {
        size: 'lg'
      }
    },
    textarea: {
      defaultVariants: {
        size: 'lg'
      }
    },
    inputTags: {
      defaultVariants: {
        size: 'lg'
      }
    },
    select: {
      defaultVariants: {
        size: 'lg'
      }
    },
    selectMenu: {
      defaultVariants: {
        size: 'lg'
      }
    },
    table: {
      // Borderless tables: rows sit on the card tone, divided by the line
      // color, and the first and last columns align with the card's padding.
      // Sticky headers take the card tone so rows scrolling beneath don't
      // show through.
      slots: {
        separator: 'bg-(--ui-border)',
        th: 'py-3 first:ps-0 last:pe-0',
        td: 'py-3 first:ps-0 last:pe-0'
      },
      variants: {
        sticky: {
          true: {
            thead: 'bg-muted'
          },
          header: {
            thead: 'bg-muted'
          }
        }
      }
    },
    alert: {
      slots: {
        // Same as card: overflow-hidden would let a flex column squash it.
        root: 'shrink-0'
      },
      defaultVariants: {
        variant: 'soft'
      }
    },
    modal: {
      slots: {
        footer: 'justify-end'
      }
    },
    separator: {
      slots: {
        label: 'caption text-muted'
      }
    },
    // Wizard step indicator: large circles, an accent glow and accent label on
    // the active step, a tinted check once a step is completed.
    stepper: {
      slots: {
        trigger: 'group-data-[state=active]:shadow-lg group-data-[state=active]:shadow-primary/30',
        title: 'text-sm text-muted group-data-[state=active]:font-semibold group-data-[state=active]:text-primary'
      },
      variants: {
        color: {
          primary: {
            trigger: 'group-data-[state=completed]:bg-primary/10 group-data-[state=completed]:text-primary'
          }
        }
      },
      defaultVariants: {
        size: 'lg'
      }
    },
    // Segmented control: the active pill takes the panel tone on a card-toned track
    // (theme default is a solid primary pill).
    tabs: {
      compoundVariants: [
        {
          color: 'primary',
          variant: 'pill',
          class: {
            list: 'bg-muted ring ring-default',
            indicator: 'bg-default shadow-xs dark:bg-inverted',
            trigger: 'data-[state=active]:text-highlighted dark:data-[state=active]:text-inverted'
          }
        }
      ]
    },
    // Only the body scrolls (the theme scrolls the whole container), so the
    // header and a wizard's Back/Next footer stay pinned. Side drawers share one width.
    drawer: {
      slots: {
        container: 'overflow-hidden',
        body: 'min-h-0 overflow-y-auto'
      },
      compoundVariants: [
        {
          direction: ['left', 'right'],
          class: {
            content: 'w-xl'
          }
        }
      ]
    }
  },
  // One icon per domain concept, next to Nuxt UI's own `ui.icons` for the
  // generic ones (loading, success, error, warning, close, reload, ...).
  icons: {
    overview: 'i-lucide-layout-dashboard',
    timeline: 'i-lucide-chart-gantt',
    graph: 'i-lucide-workflow',
    collection: 'i-lucide-library',
    components: 'i-lucide-boxes',
    executions: 'i-lucide-activity',
    agent: 'icon:agent',
    source: 'i-lucide-plug',
    destination: 'i-lucide-database',
    connection: 'i-lucide-key-round',
    config: 'i-lucide-sliders-horizontal',
    job: 'i-lucide-calendar-clock',
    hook: 'i-lucide-zap',
    asset: 'i-lucide-box',
    run: 'i-lucide-play',
    backfill: 'i-lucide-history',
    organisation: 'i-lucide-building-2',
    members: 'i-lucide-users',
    user: 'i-lucide-user',
    invite: 'i-lucide-user-plus',
    admin: 'i-lucide-shield',
    settings: 'i-lucide-settings',
    token: 'i-lucide-key',
    app: 'i-lucide-app-window',
    signIn: 'i-lucide-log-in',
    signOut: 'i-lucide-log-out',
    edit: 'i-lucide-pencil',
    delete: 'i-lucide-trash-2',
    refresh: 'i-lucide-refresh-cw',
    cancel: 'i-lucide-ban',
    enabled: 'i-lucide-power',
    status: 'i-lucide-circle-dot',
    kind: 'i-lucide-shapes',
    type: 'i-lucide-tag',
    queued: 'i-lucide-clock',
    skipped: 'i-lucide-skip-forward',
    missing: 'i-lucide-circle-dashed',
    missed: 'i-lucide-circle-slash',
    deleted: 'i-lucide-circle-off',
    drift: 'i-lucide-unplug',
    unreadable: 'i-lucide-lock',
    window: 'i-lucide-calendar-range',
    partition: 'i-lucide-layers',
    expandAll: 'i-lucide-unfold-vertical',
    collapseAll: 'i-lucide-fold-vertical',
    switcher: 'i-lucide-chevrons-up-down',
    closePanel: 'i-lucide-panel-right-close',
    quota: 'i-lucide-gauge',
    authentication: 'i-lucide-shield-check',
    conversation: 'i-lucide-message-square',
    activity: 'i-lucide-scroll-text',
    google: 'i-devicon-google'
  }
})
