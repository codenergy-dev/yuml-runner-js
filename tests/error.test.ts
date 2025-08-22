import { PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('error', () => {
  it('validate pipeline failure', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('error.json'))
    workflows.bindModules({
      'error': () => import('./pipelines/error')
    })

    const onPipelineFailed = jest.fn()
    workflows.events.on(null, onPipelineFailed, PipelineState.FAILED)

    await workflows.run('error', 'a')
    expect(onPipelineFailed).toHaveBeenCalledTimes(1)
  })
})