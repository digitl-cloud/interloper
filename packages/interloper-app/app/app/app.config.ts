export default defineAppConfig({
  ui: {
    colors: {
      primary: 'blue',
      secondary: 'gray',
      success: 'green',
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
      defaultVariants: {
        variant: 'outline'
      }
    },
    // Surface hierarchy from the Claude Design project: white content/cards,
    // sidebar bg-muted (#F7F7F8).
    dashboardSidebar: {
      slots: {
        root: 'bg-muted'
      }
    },
    navigationMenu: {
      // Design sidebar: section labels lighter than entries; 8px item padding.
      slots: {
        label: 'text-dimmed'
      },
      variants: {
        orientation: {
          vertical: {
            link: 'py-2'
          }
        }
      }
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
      slots: {
        // Unframed tables: the first column sits flush with the table edge,
        // aligned with the toolbar and caption around it.
        th: 'first:pl-0',
        td: 'first:pl-0'
      }
    },
    alert: {
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
    // Design segmented control: pill tabs get a white active pill with dark
    // text on the grey track (theme default is a solid primary pill).
    tabs: {
      compoundVariants: [
        {
          color: 'primary',
          variant: 'pill',
          class: {
            indicator: 'bg-default',
            trigger: 'data-[state=active]:text-highlighted'
          }
        }
      ]
    },
    // Design wizard drawer: 30px frame, 22px title, bordered header/footer.
    // Only the body scrolls (the theme scrolls the whole container), so the
    // header and the stepper's Back/Next footer stay pinned to the frame. The
    // gutter against those borders is the body's own padding rather than a gap
    // between the three slots: a gap sits outside the scroll area and leaves a
    // dead band the content can never reach, padding scrolls with it.
    drawer: {
      slots: {
        container: 'p-[30px] pt-[26px] gap-0 overflow-hidden',
        header: 'shrink-0 border-b border-default -mx-[30px] px-[30px] pb-5',
        title: 'text-[22px] font-bold tracking-[-0.01em]',
        body: 'flex-1 min-h-0 overflow-y-auto py-6',
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