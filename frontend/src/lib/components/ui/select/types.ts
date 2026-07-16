import type { HTMLInputAttributes } from "svelte/elements";
import type { ControlSize } from "../input/index.ts";

export type SelectItem = {
  disabled?: boolean;
  label: string;
  value: string;
};

export type SelectProps = {
  allowDeselect?: boolean;
  ariaLabel: string;
  autocomplete?: HTMLInputAttributes["autocomplete"];
  class?: string;
  contentClass?: string;
  controlSize?: ControlSize;
  disabled?: boolean;
  items: SelectItem[];
  loop?: boolean;
  name?: string;
  onOpenChange?: (open: boolean) => void;
  onValueChange?: (value: string) => void;
  open?: boolean;
  placeholder?: string;
  required?: boolean;
  triggerClass?: string;
  value?: string;
};
