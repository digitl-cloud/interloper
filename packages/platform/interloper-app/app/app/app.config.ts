export default defineAppConfig({
  ui: {
    colors: {
      primary: 'blue',
      success: 'emerald',
      warning: 'amber',
      neutral: 'neutral'
    },
    badge: {
      // `soft` = tinted fill, no ring: ids, counts beside a title and the few
      // marks that must stand out ride this default. EntityBadge is the one
      // exception, opting into `outline` so object references read as
      // references. Counts line up in tabular figures.
      slots: {
        base: 'tabular-nums'
      },
      defaultVariants: {
        size: 'md',
        variant: 'soft'
      }
    },
    // The primary action is the inverted tone (white on dark, black on light): blue stays for links,
    // focus, selection and the info status.
    button: {
      defaultVariants: {
        size: 'lg',
        color: 'neutral',
        variant: 'solid'
      }
    },
    card: {
      // Section cards: the card tone alone sets them off the panel, with no edge and no rules between
      // header, body and footer, and they pad 16px at every width, as the page does (the theme steps
      // up to 24px from `sm`). CardHeader reads `title` / `description` from here so a card with
      // controls reads the same as one using the title props.
      slots: {
        // overflow-hidden zeroes a flex item's auto min-height, so without this
        // cards squash to fit a page body instead of letting it scroll.
        root: 'shrink-0',
        header: 'sm:px-4',
        title: 'text-highlighted font-medium',
        description: 'mt-1 text-muted text-sm',
        body: 'sm:p-4 [[data-slot=header]+&]:pt-0 sm:[[data-slot=header]+&]:pt-0',
        footer: 'sm:px-4 [[data-slot=body]+&]:pt-0'
      },
      variants: {
        variant: {
          outline: {
            root: 'bg-muted ring-0 divide-y-0'
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
            root: 'bg-muted ring-0'
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
    // A page is padded and spaced 16px at every width (the theme steps up to 24px from `sm`), and its
    // navbar and toolbar keep their edges in line with it. The navbar draws no rule: it and the page
    // are one surface.
    // The agent panel slides in below the navbar: the toolbar and the body make room for it
    // (the panel sizes `agent-room` from its own stylesheet), the navbar keeps the full width.
    dashboardPanel: {
      slots: {
        body: 'agent-room transition-[margin,padding] sm:gap-4 sm:p-4'
      }
    },
    dashboardNavbar: {
      slots: {
        root: 'sm:px-4 border-b-0'
      }
    },
    // A page's toolbar (its sub-page tabs, a detail page's actions) is the first row of the page's
    // content, not a band of its own: no rule, and as far below the navbar as the content sits. It
    // keeps the theme's sideways scroll for tabs wider than a phone.
    dashboardToolbar: {
      slots: {
        root: 'agent-room transition-[margin,padding] min-h-0 border-b-0 pt-4 sm:px-4'
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
      // Groups are set apart by space, not a rule: their captions name them, and a
      // collapsed sidebar, which hides the captions, keeps the gap.
      variants: {
        orientation: {
          vertical: {
            link: 'py-2',
            separator: 'h-3 bg-transparent'
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
        // Horizontal pills (a page's sub-page tabs) are navigation, not a control: no track, the
        // current view a lighter pill, each as tall as the buttons beside them in the toolbar.
        {
          orientation: 'horizontal',
          variant: 'pill',
          class: {
            list: 'gap-1',
            item: 'py-0',
            link: 'px-3 py-2',
            linkLeadingIcon: 'size-4'
          }
        },
        {
          orientation: 'horizontal',
          variant: 'pill',
          active: true,
          class: {
            link: 'text-highlighted before:bg-elevated'
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
    // Inputs and selects are filled, the same fill at rest and on hover, and ringed only while focused.
    input: {
      variants: {
        variant: {
          soft: 'bg-elevated hover:bg-elevated'
        }
      },
      compoundVariants: [
        {
          color: 'primary',
          variant: 'soft',
          class: 'focus-visible:ring focus-visible:ring-inset focus-visible:ring-primary'
        }
      ],
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    textarea: {
      variants: {
        variant: {
          soft: 'bg-elevated hover:bg-elevated'
        }
      },
      compoundVariants: [
        {
          color: 'primary',
          variant: 'soft',
          class: 'focus-visible:ring focus-visible:ring-inset focus-visible:ring-primary'
        }
      ],
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    inputTags: {
      variants: {
        variant: {
          soft: 'bg-elevated hover:bg-elevated'
        }
      },
      compoundVariants: [
        {
          color: 'primary',
          variant: 'soft',
          class: 'has-focus-visible:ring has-focus-visible:ring-inset has-focus-visible:ring-primary'
        }
      ],
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    select: {
      variants: {
        variant: {
          soft: 'bg-elevated hover:bg-elevated'
        }
      },
      compoundVariants: [
        {
          color: 'primary',
          variant: 'soft',
          class: 'focus-visible:ring focus-visible:ring-inset focus-visible:ring-primary'
        }
      ],
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    selectMenu: {
      variants: {
        variant: {
          soft: 'bg-elevated hover:bg-elevated'
        }
      },
      compoundVariants: [
        {
          color: 'primary',
          variant: 'soft',
          class: 'focus-visible:ring focus-visible:ring-inset focus-visible:ring-primary'
        }
      ],
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    table: {
      // Borderless tables: rows sit on the card tone, divided by the line
      // color. The table reaches 8px into the card's padding and its first and
      // last cells pad themselves back, so the text aligns with the card's
      // while a row's highlight (and a sortable header's) runs past it.
      // Headers label quietly; the first column carries the row's name. A row
      // is a `group/row`, so what it holds can answer its hover (the options
      // menu). Sticky headers take the card tone so rows scrolling beneath
      // don't show through.
      slots: {
        root: '-mx-2',
        separator: 'bg-(--ui-border)',
        tr: 'group/row',
        th: 'py-3 first:ps-2 last:pe-2 text-muted font-normal',
        td: 'py-3 first:ps-2 last:pe-2 first:text-highlighted first:font-medium'
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
    // Section headers above a card: title and description take the row and wrap only when it runs
    // out; the actions keep their own width (the theme splits the row in two equal columns).
    pageCard: {
      slots: {
        title: 'font-medium'
      },
      variants: {
        orientation: {
          horizontal: {
            container: 'lg:flex lg:flex-row'
          }
        }
      }
    },
    modal: {
      slots: {
        title: 'font-medium',
        footer: 'justify-end'
      }
    },
    separator: {
      slots: {
        label: 'caption text-muted'
      }
    },
    // The wizard's step indicator keeps Nuxt UI's look; its step content sits a section's gap below it.
    stepper: {
      slots: {
        root: 'gap-6'
      }
    },
    // Segmented control (a filter, a view switch): a track filled like the inputs, the chosen value a
    // raised chip (theme default is a solid primary pill). A track wider than its room scrolls
    // sideways. It stands as tall as the controls beside it: a toolbar's inputs and buttons (sm,
    // 36px), a card header's small buttons (xs, 28px).
    tabs: {
      compoundVariants: [
        {
          color: 'primary',
          variant: 'pill',
          class: {
            root: 'min-w-0',
            list: 'max-w-full overflow-x-auto bg-elevated',
            indicator: 'bg-default shadow-xs dark:bg-accented',
            trigger: 'shrink-0 data-[state=active]:text-highlighted'
          }
        },
        {
          color: 'primary',
          variant: 'pill',
          size: 'sm',
          class: {
            trigger: 'h-7'
          }
        },
        {
          color: 'primary',
          variant: 'pill',
          size: 'xs',
          class: {
            list: 'p-0.5',
            trigger: 'h-6'
          }
        },
        // Vertical, the pills are a list (the phone's filter sheet): full-width rows, counts at the end.
        {
          color: 'primary',
          variant: 'pill',
          orientation: 'vertical',
          class: {
            root: 'w-full',
            list: 'w-full',
            trigger: 'justify-start',
            trailingBadge: 'ms-auto'
          }
        }
      ]
    },
    // Only the body scrolls (the theme scrolls the whole container), so the
    // header and a wizard's Back/Next footer stay pinned. The body reaches into the
    // container's padding (p-4) and pads itself back, so the rings and focus outlines
    // of what scrolls in it are not clipped at its edge. Side drawers share one width.
    drawer: {
      slots: {
        title: 'font-medium',
        container: 'overflow-hidden',
        body: '-mx-4 -my-1 min-h-0 overflow-y-auto px-4 py-1'
      },
      compoundVariants: [
        // Side drawers (wizards, edit forms) stack header, body and footer a section apart.
        {
          direction: ['left', 'right'],
          class: {
            container: 'gap-6',
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
    table: 'i-lucide-table',
    collection: 'i-lucide-library',
    components: 'i-lucide-boxes',
    executions: 'i-lucide-activity',
    agent: 'icon:agent',
    source: 'i-lucide-plug',
    destination: 'i-lucide-database',
    connection: 'i-lucide-key-round',
    config: 'i-lucide-sliders-horizontal',
    filter: 'i-lucide-list-filter',
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
    options: 'i-lucide-ellipsis-vertical',
    edit: 'i-lucide-pencil',
    served: 'i-lucide-file-lock',
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
    google: 'i-devicon-google',
    microsoft: 'icon:microsoft'
  }
})
