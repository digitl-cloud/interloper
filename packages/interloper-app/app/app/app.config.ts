export default defineAppConfig({
  ui: {
    colors: {
      primary: 'blue',
      secondary: 'gray',
      success: 'emerald',
      warning: 'amber',
      error: 'red',
      info: 'blue',
    },
    badge: {
      // `soft` = tinted fill, no ring: badges carry no border. Statuses, tags,
      // counts and log levels all ride this default; EntityBadge is the one
      // documented exception, opting into `outline` so object references read
      // as references.
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
      // Section cards: container tone, no rules between header, body and
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
    dashboardSidebar: {
      slots: {
        root: 'bg-muted',
        // The mobile slideover renders through `content`, not `root`.
        content: 'bg-muted'
      }
    },
    // The body ends as close to the window's bottom edge as the agent card (AGENT_PANEL_INSET), so their bottoms line up.
    dashboardPanel: {
      slots: {
        body: 'pb-2 sm:pb-2'
      }
    },
    // Borderless, the navbar keeps no space under its controls: the page's content sits as far below them as
    // it sits from the panel's sides. The top padding keeps the controls centred on the sidebar header's line.
    dashboardNavbar: {
      slots: {
        root: 'h-auto border-b-0 pt-2.5'
      }
    },
    // A page's toolbar (its view tabs, a run's actions) reads as part of the header, not a band of its own:
    // it sits as far below the header as the body's content sits below it. Its content scrolls itself.
    dashboardToolbar: {
      slots: {
        root: 'min-h-0 overflow-visible border-b-0 pt-4 sm:pt-6'
      }
    },
    navigationMenu: {
      slots: {
        label: 'text-highlighted'
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
        }
      ]
    },
    input: {
      defaultVariants: {
        size: 'lg',
        variant: 'outline'
      }
    },
    textarea: {
      defaultVariants: {
        size: 'lg',
        variant: 'outline'
      }
    },
    inputTags: {
      defaultVariants: {
        size: 'lg',
        variant: 'outline'
      }
    },
    tag: {
      defaultVariants: {
        size: 'lg',
        variant: 'soft'
      }
    },
    select: {
      defaultVariants: {
        size: 'lg',
        variant: 'outline'
      }
    },
    selectMenu: {
      defaultVariants: {
        size: 'lg',
        variant: 'outline'
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
    // Design wizard step indicator: 48px circles, accent glow + accent
    // label on the active step, tinted check circle once completed.
    stepper: {
      slots: {
        root: 'gap-7',
        trigger: 'text-dimmed group-data-[state=active]:shadow-lg group-data-[state=active]:shadow-primary/30',
        title: 'text-[13px] font-medium text-dimmed group-data-[state=active]:text-primary group-data-[state=active]:font-semibold group-data-[state=completed]:text-muted'
      },
      variants: {
        size: {
          md: {
            trigger: 'size-12',
            icon: 'size-[22px]'
          }
        },
        color: {
          primary: {
            trigger: 'group-data-[state=completed]:bg-primary/15 group-data-[state=completed]:text-primary'
          }
        }
      }
    },
    // Design mono section label on labeled separators.
    separator: {
      slots: {
        label: 'eyebrow text-dimmed'
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
    // Design wizard drawer: 30px frame, 22px title, bordered header/footer.
    // Only the body scrolls (the theme scrolls the whole container), so the
    // header and the stepper's Back/Next footer stay pinned to the frame. The
    // gutter against those borders is the body's own padding rather than a gap
    // between the three slots: a gap sits outside the scroll area and leaves a
    // dead band the content can never reach, padding scrolls with it. The body
    // bleeds to the frame's sides like the header and footer, so its scrollbar
    // sits on the drawer's edge rather than inside the frame padding.
    drawer: {
      slots: {
        container: 'p-[30px] pt-[26px] gap-0 overflow-hidden',
        header: 'shrink-0 border-b border-default -mx-[30px] px-[30px] pb-5',
        title: 'text-[22px] font-bold tracking-[-0.01em]',
        body: 'flex-1 min-h-0 overflow-y-auto -mx-[30px] px-[30px] py-6',
        footer: 'shrink-0 border-t border-default -mx-[30px] -mb-[30px] px-[30px] py-4'
      },
      compoundVariants: [
        // Square edges: the theme rounds the drawer's inner edge per direction
        // (`rounded-l-lg` for a right drawer). Those per-direction compounds win
        // the class merge over `slots.content`, so the reset has to be one too.
        {
          inset: false,
          class: {
            content: 'rounded-none'
          }
        }
      ]
    }
  }
})
