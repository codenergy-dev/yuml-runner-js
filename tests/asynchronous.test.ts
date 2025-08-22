import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('asynchronous', () => {
  it('run asynchronous pipeline', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('asynchronous.json'))
    workflows.bindModules({
      'asynchronous': () => import('./pipelines/asynchronous')
    })

    const onPipelineDone = jest.fn()
    workflows.events.on(null, onPipelineDone)

    await workflows.run('asynchronous', 'a')
    expect(onPipelineDone).toHaveBeenCalledTimes(3)
  })
})