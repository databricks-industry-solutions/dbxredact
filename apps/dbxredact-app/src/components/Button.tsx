import type { ButtonHTMLAttributes, ReactNode } from "react";

export type ButtonVariant = "primary" | "success" | "secondary" | "danger" | "ghost";

const VARIANT_CLASS: Record<ButtonVariant, string> = {
  primary: "btn-primary",
  success: "btn-success",
  secondary: "btn-secondary",
  danger: "btn-danger",
  ghost: "btn-ghost",
};

interface Props extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  /** When true, disables the button and shows `loadingText` (falls back to children). */
  loading?: boolean;
  loadingText?: ReactNode;
  children: ReactNode;
}

/**
 * Standard button. Composes the existing `.btn-*` CSS component classes so it stays
 * visually identical to hand-written `<button className="btn-primary">` markup, while
 * standardizing the disabled-while-loading + "Saving…/Launching…" label pattern.
 */
export default function Button({
  variant = "primary",
  loading = false,
  loadingText,
  disabled,
  className = "",
  children,
  ...rest
}: Props) {
  return (
    <button
      className={`${VARIANT_CLASS[variant]} ${className}`}
      disabled={disabled || loading}
      {...rest}
    >
      {loading ? loadingText ?? children : children}
    </button>
  );
}
