import { Pipeline, PipelineState } from '../src/pipeline'
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
    
    await workflows.run('path', 'a')
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
    
    await workflows.run('path', 'd')
    expect(onNotEntrypointPipeline).toHaveBeenCalledTimes(1)
  })

  it('compare inputs and outputs between workflows', async () => {
    const workflows = Workflows.fromJson([
      ...readWorkflowJson('output.json'),
      ...readWorkflowJson('path.json'),
    ])
    workflows.bindModules({
      'output': () => import('./pipelines/output'),
      'path': () => import('./pipelines/path')
    })

    const pipelines: Pipeline[] = []
    workflows.events.on(null, (p) => pipelines.push(p))
    workflows.events.on(null, (p) => pipelines.push(p), PipelineState.SKIP)

    await workflows.run('path', 'f')
    expect(pipelines.find(p => p.path == 'output' && p.name == 'output.c')?.input)
      .toStrictEqual(pipelines.find(p => p.workflow == 'output' && p.name == 'c')?.input)
    expect(pipelines.find(p => p.path == 'output' && p.name == 'output.c')?.output)
      .toStrictEqual(pipelines.find(p => p.workflow == 'output' && p.name == 'c')?.output)
  })
})