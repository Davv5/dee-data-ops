import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

type CardProps = {
  children: ReactNode;
  className?: string;
};

export function Card({ children, className }: CardProps) {
  return (
    <section
      className={cn(
        "rounded-lg border border-[#dedbd2] bg-white shadow-sm",
        className,
      )}
    >
      {children}
    </section>
  );
}

export function CardHeader({ children, className }: CardProps) {
  return <div className={cn("border-b border-[#ece9e1] p-4", className)}>{children}</div>;
}

export function CardBody({ children, className }: CardProps) {
  return <div className={cn("p-4", className)}>{children}</div>;
}
