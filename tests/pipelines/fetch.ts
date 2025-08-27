export function fetchUrl() {
  return fetch('https://www.google.com/')
}

export function responseStatusOk(response: Response) {
  if (response.status == 200) return response
  return
}

export async function responseText(response: Response)  {
  const text = await response.text()
  return { response: text.substring(0, 128) + '...' }
}