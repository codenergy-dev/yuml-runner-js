import { Pipeline } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('fetch', () => {
  it('validate fetch call and response', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('fetch.json'))
    workflows.bindModules({
      'fetch': () => import('./pipelines/fetch')
    })

    var pipelines: Pipeline[] = []
    workflows.events.on(null, (p) => pipelines.push(p))

    await workflows.run('fetch', 'fetchUrl')
    expect(pipelines.map(p => p.name).join('->')).toBe('fetchUrl->responseStatusOk->responseText')
    expect(pipelines.find(p => p.name == 'responseStatusOk')?.input).toBeInstanceOf(Response)
  })
})