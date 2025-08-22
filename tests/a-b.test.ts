import { Pipeline, PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('a-b', () => {
  it('parse workflow', () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    expect(workflows.pipelines.length).toBe(2)
  })
  
  it('bind workflow modules and run pipelines', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const onPipelineDone = jest.fn()
    workflows.events.on(null, onPipelineDone)

    await workflows.run('a-b', 'a')
    expect(onPipelineDone).toHaveBeenCalledTimes(2)
  })
  
  it('validate pipeline args', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const pipelines: Pipeline[] = []
    workflows.events.on(null, (p) => pipelines.push(p))

    await workflows.run('a-b', 'a')
    expect(pipelines.find(p => p.name == 'a')?.args['foo']).toBe('bar')
  })

  it('validate pipelines execution sequence', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const history: string[] = []
    workflows.events.on(null, (pipeline) => history.push(pipeline.name))

    await workflows.run('a-b', 'a')
    
    expect(history[0]).toBe('a')
    expect(history[1]).toBe('b')
  })

  it('validate pipeline event emitter', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const onPipelineExec = jest.fn()
    workflows.events.on(null, onPipelineExec, PipelineState.EXEC)

    const onPipelineDone = jest.fn()
    workflows.events.on(null, onPipelineDone, PipelineState.DONE)

    const onPipelineWait = jest.fn()
    workflows.events.on(null, onPipelineWait, PipelineState.WAIT)

    const onPipelineAny = jest.fn()
    workflows.events.on(null, onPipelineAny, null)

    const onPipelineA = jest.fn()
    workflows.events.on('a', onPipelineA)

    const onPipelineB = jest.fn()
    workflows.events.on('b', onPipelineB)

    await workflows.run('a-b', 'a')
    
    expect(onPipelineExec).toHaveBeenCalledTimes(2)
    expect(onPipelineDone).toHaveBeenCalledTimes(2)
    expect(onPipelineWait).toHaveBeenCalledTimes(0)
    expect(onPipelineAny).toHaveBeenCalledTimes(4)
    expect(onPipelineA).toHaveBeenCalledTimes(1)
    expect(onPipelineB).toHaveBeenCalledTimes(1)
  })

  it('validate pipeline entrypoint', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const onPipelineDone = jest.fn()
    workflows.events.on(null, onPipelineDone)

    await workflows.run('a-b', 'b')
    expect(onPipelineDone).toHaveBeenCalledTimes(0)
  })
})