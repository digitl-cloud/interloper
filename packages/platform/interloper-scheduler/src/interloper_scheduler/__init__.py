"""Scheduler: the loops that turn stored component intent into runs, and run them.

Four controllers share one :class:`Controller` loop skeleton and run as
cluster singletons: cron creates runs from job schedules, hooks fires them in
reaction to terminal runs, the queue worker assembles queued runs and
dispatches them through a :class:`Launcher`, and the reaper fails the runs
that will never end on their own. A launched run reports back on its own,
through the API or, in-process, the store's ingest.

The package is imported whenever one of its settings sections is read or
``interloper scheduler`` parses its arguments, so it imports nothing here:
each controller is imported from its module, and `scheduler_services` from
`interloper_scheduler.services`.
"""
