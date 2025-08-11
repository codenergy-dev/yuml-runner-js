import { PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('asynchronous', () => {
  it('run asynchronous pipeline', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('asynchronous.json'))
    workflows.bindModules({
      'asynchronous': () => import('./pipelines/asynchronous')
    })

    const pipelines = await workflows.run('asynchronous', 'a')
    expect(pipelines.filter(p => p.state == PipelineState.DONE).length).toBe(3)
  })
})