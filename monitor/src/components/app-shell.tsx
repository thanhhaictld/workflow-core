"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { Activity, BookOpen, ChevronRight, CircleHelp, GitBranch, ListTree, Menu, Moon, Sun, X } from "lucide-react";
import { Button, cn } from "./ui";

const documentation = "https://github.com/thanhhaictld/workflow-core/blob/feat/workflow-monitor-dashboard/scaling/docs/execution-history.md";

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isDetail = /^\/workflows\/[^/]+/.test(pathname);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [dark, setDark] = useState(false);

  useEffect(() => {
    const preference = window.localStorage.getItem("workflow-monitor-theme") === "dark";
    setDark(preference);
    document.documentElement.classList.toggle("dark", preference);
  }, []);

  function toggleTheme() {
    setDark(value => {
      const next = !value;
      document.documentElement.classList.toggle("dark", next);
      window.localStorage.setItem("workflow-monitor-theme", next ? "dark" : "light");
      return next;
    });
  }

  return <div className="flex min-h-screen">
    {sidebarOpen && <button aria-label="Close navigation" onClick={() => setSidebarOpen(false)}
      className="fixed inset-0 z-30 bg-black/40 lg:hidden" />}
    <aside className={cn("fixed inset-y-0 left-0 z-40 flex w-64 flex-col border-r border-sidebar-border bg-sidebar",
      "transition-transform lg:sticky lg:top-0 lg:h-screen lg:translate-x-0",
      sidebarOpen ? "translate-x-0" : "-translate-x-full")}>
      <div className="flex h-[69px] shrink-0 items-center gap-3 border-b border-sidebar-border px-5">
        <div className="grid size-9 place-items-center rounded-lg bg-primary text-primary-foreground">
          <GitBranch size={18} strokeWidth={2.2} />
        </div>
        <div className="min-w-0 leading-tight">
          <div className="text-sm font-bold tracking-tight">Workflow Core</div>
          <div className="text-xs text-muted-foreground">Execution monitor</div>
        </div>
        <button className="ml-auto rounded p-1 lg:hidden" aria-label="Close sidebar" onClick={() => setSidebarOpen(false)}>
          <X size={18} />
        </button>
      </div>
      <nav className="flex-1 px-3 py-6">
        <div className="px-3 pb-3 text-[10px] font-bold uppercase tracking-[0.16em] text-muted-foreground">Workspace</div>
        <Link href="/workflows" onClick={() => setSidebarOpen(false)}
          className={cn("flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-colors",
            pathname.startsWith("/workflows") ? "bg-primary/10 font-semibold text-foreground" : "text-muted-foreground hover:bg-muted")}>
          <Activity size={17} /><span>Workflow runs</span><ChevronRight size={15} className="ml-auto opacity-50" />
        </Link>
        <div className="mt-8 px-3 pb-3 text-[10px] font-bold uppercase tracking-[0.16em] text-muted-foreground">Resources</div>
        <a href={documentation} target="_blank" rel="noreferrer"
          className="flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm text-muted-foreground hover:bg-muted">
          <BookOpen size={17} />Execution history docs
        </a>
      </nav>
      <div className="m-3 rounded-xl border border-border bg-card p-3">
        <div className="flex items-center gap-2 text-xs font-semibold"><ListTree size={15} /> PostgreSQL journal</div>
        <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
          Read-only workflow inspection; development-only sample onboarding controls.
        </p>
      </div>
      <div className="border-t border-sidebar-border px-5 py-4 text-xs text-muted-foreground">Distributed runtime · experimental</div>
    </aside>

    <div className="flex min-w-0 flex-1 flex-col">
      <header className="sticky top-0 z-20 flex h-[69px] shrink-0 items-center justify-between gap-4 border-b border-border bg-card/95 px-4 backdrop-blur sm:px-7">
        <div className="flex min-w-0 items-center gap-3">
          <Button variant="ghost" size="sm" aria-label="Open navigation" className="lg:hidden" onClick={() => setSidebarOpen(true)}>
            <Menu size={19} />
          </Button>
          <div className="flex min-w-0 items-center gap-2 truncate text-sm">
            <span className="hidden text-muted-foreground sm:inline">Monitor</span>
            <ChevronRight size={14} className="hidden text-muted-foreground sm:inline" />
            <span className="font-medium">{isDetail ? "Execution detail" : "Workflow runs"}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="hidden items-center gap-1.5 rounded-full border border-border px-2.5 py-1.5 text-xs text-muted-foreground sm:flex">
            <span className="size-1.5 rounded-full bg-emerald-500" />Demo lab
          </span>
          <Button variant="ghost" size="sm" onClick={toggleTheme} aria-label={dark ? "Switch to light theme" : "Switch to dark theme"}>
            {dark ? <Sun size={17} /> : <Moon size={17} />}
          </Button>
          <a title="Workflow Core documentation" href={documentation} target="_blank" rel="noreferrer"
            className="rounded-md p-2 text-muted-foreground hover:bg-muted"><CircleHelp size={17} /></a>
        </div>
      </header>
      <main className="mx-auto w-full max-w-[1680px] flex-1 px-4 py-7 sm:px-7 lg:py-9">{children}</main>
      <footer className="border-t border-border px-7 py-4 text-xs text-muted-foreground">
        Workflow Core monitoring · real PostgreSQL history · simulated demo steps
      </footer>
    </div>
  </div>;
}
