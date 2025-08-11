import { PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('error', () => {
  it('validate pipeline failure', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('error.json'))
    workflows.bindModules({
      'error': () => import('./pipelines/error')
    })

    const pipelines = await workflows.run('error', 'a')
    expect(pipelines.filter(p => p.state == PipelineState.FAILED).length).toBe(1)
  })
})