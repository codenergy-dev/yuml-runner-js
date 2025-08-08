import { resolve } from 'path'
import { readFileSync } from 'fs'

export function readWorkflowJson(...paths: string[]) {
  const jsonPath = resolve(__dirname, '..', 'fixtures', 'workflows', ...paths)
  const jsonRaw = readFileSync(jsonPath, 'utf-8')
  const jsonData = JSON.parse(jsonRaw)
  return jsonData
}