export function a() {
  return {}
}

export async function asynchronous() {
  await new Promise(r => setTimeout(r, 1000))
  return {}
}

export function b() {
  return
}