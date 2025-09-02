interface FetchResponse {
  response: Response
}

export async function fetchUrl() {
  return { response: await fetch('https://www.google.com/') } as FetchResponse
}

export function responseStatusOk({ response }: FetchResponse) {
  return response.status == 200
}

export async function responseText({ response }: FetchResponse)  {
  const text = await response.text()
  return { response: text.substring(0, 128) + '...' }
}