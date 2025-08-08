export type ForEachCount = { count: number }

export function a({ count }: ForEachCount) {
  return { count }
}

export function forEach({ count }: ForEachCount) {
  return Array(count).map(e => {})
}

export function b() {
  return
}