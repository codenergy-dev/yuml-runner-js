import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('event', () => {
  it('trigger pipeline from another workflow event', async () => {
    const workflows = Workflows.fromJson([
      ...readWorkflowJson('a-b.json'),
      ...readWorkflowJson('event.json'),
    ])
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b'),
      'event': () => import('./pipelines/event')
    })

    const onPipelineEvent = jest.fn()
    workflows.events.on('event', onPipelineEvent)

    await workflows.run('a-b', 'a')
    
    // Awaits the event to trigger the pipeline
    await new Promise((r) => setTimeout(r, 100))
    
    expect(onPipelineEvent).toHaveBeenCalledTimes(1)
  })
})