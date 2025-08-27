export type ForEachCount = { count: number }

export function a({ count }: ForEachCount) {
  return { count }
}

export function forEach({ count }: ForEachCount) {
  return Array.from({ length: count }, (_, i) => ({ i }))
}

export function b() {
  return
}