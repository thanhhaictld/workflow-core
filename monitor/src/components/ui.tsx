import type { ButtonHTMLAttributes, HTMLAttributes, InputHTMLAttributes, ReactNode, SelectHTMLAttributes } from "react";
import clsx from "clsx";

export function cn(...classes: Parameters<typeof clsx>) { return clsx(...classes); }

export function buttonClasses(variant: "primary" | "outline" | "ghost" = "outline", size: "md" | "sm" = "md") {
  return cn(
    "inline-flex shrink-0 items-center justify-center gap-2 rounded-lg border text-sm font-medium",
    "transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-500 disabled:opacity-45",
    size === "sm" ? "h-8 px-3" : "h-9 px-3.5",
    variant === "primary" && "border-primary bg-primary text-primary-foreground hover:opacity-90",
    variant === "outline" && "border-border bg-card hover:bg-muted",
    variant === "ghost" && "border-transparent bg-transparent hover:bg-muted"
  );
}

export function Button({ variant = "outline", size = "md", className, ...props }:
  ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "outline" | "ghost"; size?: "md" | "sm" }) {
  return <button className={cn(buttonClasses(variant, size), className)} {...props} />;
}

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <section className={cn("rounded-xl border border-border bg-card shadow-[0_1px_3px_0_#11182709]", className)} {...props} />;
}

export function CardHeader({ title, description, action }: { title: string; description?: string; action?: ReactNode }) {
  return <header className="flex flex-wrap items-start justify-between gap-2 border-b border-border px-5 py-4">
    <div><h2 className="text-sm font-semibold">{title}</h2>{description && <p className="mt-1 text-xs text-muted-foreground">{description}</p>}</div>
    {action}
  </header>;
}

export function Input({ className, ...props }: InputHTMLAttributes<HTMLInputElement>) {
  return <input className={cn("h-9 w-full rounded-lg border border-border bg-card px-3 text-sm",
    "placeholder:text-muted-foreground focus:outline-2 focus:outline-offset-0 focus:outline-blue-500", className)} {...props} />;
}

export function Select({ className, ...props }: SelectHTMLAttributes<HTMLSelectElement>) {
  return <select className={cn("h-9 rounded-lg border border-border bg-card px-3 text-sm", "focus:outline-2 focus:outline-blue-500", className)} {...props} />;
}

type Tone = "green" | "blue" | "amber" | "red" | "slate" | "purple";
export function Badge({ children, tone = "slate", className }: { children: ReactNode; tone?: Tone; className?: string }) {
  const tones: Record<Tone, string> = {
    green: "bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
    blue: "bg-blue-500/10 text-blue-700 dark:text-blue-300",
    amber: "bg-amber-500/10 text-amber-700 dark:text-amber-300",
    red: "bg-red-500/10 text-red-700 dark:text-red-300",
    slate: "bg-slate-500/10 text-slate-600 dark:text-slate-300",
    purple: "bg-violet-500/10 text-violet-700 dark:text-violet-300"
  };
  return <span className={cn("inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-semibold whitespace-nowrap",
    tones[tone], className)}>{children}</span>;
}

export function StatusBadge({ status }: { status: string }) {
  const normalized = status.toLowerCase();
  const tone: Tone = ["complete", "completed", "success"].includes(normalized) ? "green"
    : ["runnable", "running", "leased", "active"].includes(normalized) ? "blue"
    : ["waitingforevent", "pending", "ready", "sleeping", "suspended"].includes(normalized) ? "amber"
    : ["failed", "deadletter", "terminated", "error", "unknown"].includes(normalized) ? "red"
    : "slate";
  return <Badge tone={tone}><span className={cn("size-1.5 rounded-full bg-current")} />{status}</Badge>;
}

export function EmptyState({ title, description, action }: { title: string; description: string; action?: ReactNode }) {
  return <div className="flex min-h-48 flex-col items-center justify-center gap-2 px-5 py-10 text-center">
    <strong className="text-sm font-semibold">{title}</strong>
    <p className="max-w-md text-xs leading-5 text-muted-foreground">{description}</p>
    {action}
  </div>;
}
