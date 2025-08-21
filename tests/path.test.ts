import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('path', () => {
  it('validate pipeline call to another workflow that is an entrypoint', async () => {
    const workflows = Workflows.fromJson([
      ...readWorkflowJson('a-b-c-d-e.json'),
      ...readWorkflowJson('path.json'),
    ])
    workflows.bindModules({
      'a-b-c-d-e': () => import('./pipelines/a-b-c-d-e'),
      'path': () => import('./pipelines/path')
    })

    const anotherWorkflowHistory: string[] = []
    workflows.events.on(null, (pipeline) => {
      if (pipeline.workflow == 'a-b-c-d-e') {
        anotherWorkflowHistory.push(pipeline.name)
      }
    })
    
    const pipelines = await workflows.run('path', 'a')
    expect(anotherWorkflowHistory.join('->')).toBe('a->b')
  })

  it('validate pipeline call to another workflow that is NOT an entrypoint', async () => {
    const workflows = Workflows.fromJson([
      ...readWorkflowJson('a-b-c-d-e.json'),
      ...readWorkflowJson('path.json'),
    ])
    workflows.bindModules({
      'a-b-c-d-e': () => import('./pipelines/a-b-c-d-e'),
      'path': () => import('./pipelines/path')
    })

    const onNotEntrypointPipeline = jest.fn()
    workflows.events.on(null, (pipeline) => {
      if (pipeline.path == 'a-b-c-d-e') {
        onNotEntrypointPipeline()
      }
    })
    
    const pipelines = await workflows.run('path', 'd')
    expect(onNotEntrypointPipeline).toHaveBeenCalledTimes(1)
  })
})