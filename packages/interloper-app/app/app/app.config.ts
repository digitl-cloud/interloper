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
    // Surface hierarchy from the Claude Design project: white header/cards,
    // sidebar bg-muted (#F7F7F8).
    dashboardSidebar: {
      slots: {
        root: 'bg-muted'
      }
    },
    navigationMenu: {
      // Design sidebar hierarchy: section labels lighter (#86868b) than
      // menu entries (#3f3f44 text, #86868b icons).
      slots: {
        label: 'text-gray-700 dark:text-gray-800'
      },
      compoundVariants: [
        // Sidebar active item: accent-tinted pill (a gray pill would be
        // invisible on the gray sidebar).
        {
          orientation: 'vertical',
          variant: 'pill',
          active: true,
          highlight: false,
          class: {
            link: 'before:bg-primary/10 text-primary',
            linkLeadingIcon: 'text-primary group-data-[state=open]:text-primary'
          }
        },
        {
          orientation: 'vertical',
          variant: 'pill',
          active: false,
          disabled: false,
          class: {
            link: 'text-toned',
            linkLeadingIcon: 'text-gray-700'
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
      slots: {
        // Design table treatment: bordered card, #FAFAFB header band,
        // #86868b header text, #F2F3F5 row dividers (lighter than the
        // card border).
        root: 'border border-default rounded-xl bg-default',
        thead: 'bg-(--ui-bg-band)',
        th: 'text-gray-700',
        tbody: 'divide-(--ui-border-muted)'
      },
      variants: {
        sticky: {
          true: {
            // The sticky variant's own bg-default/75 wins the class merge
            // over slots.thead, so restate the header band here (opaque).
            thead: 'bg-(--ui-bg-band) backdrop-blur-none'
          }
        }
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
    drawer: {
      slots: {
        container: 'p-[30px] pt-[26px] gap-6',
        header: 'border-b border-default -mx-[30px] px-[30px] pb-5',
        title: 'text-[22px] font-bold tracking-[-0.01em]',
        footer: 'border-t border-default -mx-[30px] -mb-[30px] px-[30px] py-4'
      }
    }
  }
})